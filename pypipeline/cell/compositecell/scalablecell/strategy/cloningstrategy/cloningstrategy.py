from typing import Optional, Dict, Sequence, TYPE_CHECKING, Type
from threading import Thread, Event, Lock

from pypipeline.cell.compositecell.scalablecell.strategy.ascalingstrategy import AScalingStrategy
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonethread import CloneThread
from pypipeline.exceptions import InvalidStateException, InvalidInputException
from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment


class CloningStrategy(AScalingStrategy):
    """
    Cloning strategy class.

    A scaling strategy determines how a scalable cell should be executed. ATM 2 strategies are implemented:
     - the CloningStrategy (default): allows to scale up using clones of the internal cell
     - the NoScalingStrategy: doesn't allow scaling and executes the scalable cell's internal cell in the mainthread,
       just like a Pipeline. Useful for debugging.
    """

    def __init__(self, scalable_cell_deployment: "ScalableCellDeployment"):
        """
        Args:
            scalable_cell_deployment: the scalable cell deployment where this scaling strategy belongs to.
        Raises:
            NotDeployableException – if the internal cell cannot be deployed.
            AlreadyDeployedException – if the internal cell is already deployed.
            NotDeployableException – if the internal cell cannot be deployed.
            Exception – any exception that the user may raise when overriding _on_deploy or _on_undeploy
        """
        super(CloningStrategy, self).__init__(scalable_cell_deployment)
        self.__clones: Dict[ICloneCell, Thread] = {}
        self.__clones_are_active: Dict[ICloneCell, Event] = {}    # can be set by both thread manager and thread
        self.__clones_must_quit: Dict[ICloneCell, Event] = {}     # should only be set by thread manager (this)
        self.__clones_have_paused: Dict[ICloneCell, Event] = {}   # should only be set by thread itself
        self.__worker_id_counter: int = 0
        self.__has_started: bool = False
        self.__queue_reservation_lock = Lock()

    @classmethod
    def create(cls, scalable_cell_deployment: "ScalableCellDeployment") -> "CloningStrategy":
        return CloningStrategy(scalable_cell_deployment)

    def __get_queue_reservation_lock(self) -> Lock:
        """
        Returns:
            The lock which should be acquired while reserving a spot in the output queue and while pulling
            the inputs. Makes sure no 2 clone threads can pull the inputs at the same time.
        """
        return self.__queue_reservation_lock

    # ------ Clones ------

    def get_all_clones(self) -> Sequence[ICloneCell]:
        """
        Returns:
            All clone cells that are created from the original cell.
        """
        return tuple(self.__clones.keys())

    def add_clone(self, method: Type[ICloneCell]) -> None:      # TODO create_clone would be a better name?
        internal_cell = self.get_internal_cell()
        scalable_cell_name = self.get_scalable_cell_deployment().get_scalable_cell().get_full_name()
        scalable_cell_name = str(scalable_cell_name).replace(".", "-")
        clone_name = f"{scalable_cell_name} - worker{self.__worker_id_counter}"
        clone = method.create(internal_cell, clone_name)    # TODO may raise exceptions
        self.logger.info(f"{self} created clone: {clone}")
        self.__add_clone(clone)

    def __add_clone(self, clone: ICloneCell) -> None:
        raise_if_not(self.__can_have_as_clone(clone), InvalidInputException)
        check_quit_interval = self.get_scalable_cell_deployment().get_scalable_cell().config_check_quit_interval.pull()
        scalable_cell_name = self.get_scalable_cell_deployment().get_scalable_cell().get_full_name()
        scalable_cell_name = str(scalable_cell_name).replace(".", "-")
        clone_worker_name = f"{scalable_cell_name} - worker{self.__worker_id_counter}"
        is_active_event = Event()
        must_quit_event = Event()
        has_paused_event = Event()
        clone_worker = CloneThread(clone,
                                   self.get_scalable_cell_deployment().get_scalable_cell().get_input_ports(),
                                   self.get_scalable_cell_deployment().get_scalable_cell().get_output_ports(),
                                   is_active_event,
                                   must_quit_event,
                                   has_paused_event,
                                   self.__get_queue_reservation_lock(),
                                   self.get_scalable_cell_deployment().get_output_queue(),
                                   check_quit_interval,
                                   clone_worker_name)
        self.__worker_id_counter += 1
        self.__clones[clone] = clone_worker
        self.__clones_are_active[clone] = is_active_event
        self.__clones_must_quit[clone] = must_quit_event
        self.__clones_have_paused[clone] = has_paused_event
        clone.deploy()      # TODO may raise exceptions
        clone_worker.start()
        if self.__has_started:
            is_active_event.set()

    def remove_clone(self, method: Type[ICloneCell]) -> None:
        the_chosen_one: Optional[ICloneCell] = None
        for clone in self.__clones.keys():
            if isinstance(clone, method):
                the_chosen_one = clone
                break
        if the_chosen_one is None:
            raise ValueError(f"{self} has no clone of type {method} that can be removed. Available types are: "
                             f"{self.get_scalable_cell_deployment().get_scalable_cell().get_all_clone_types()}")
        self.__remove_clone(the_chosen_one)

    def __remove_clone(self, clone: ICloneCell) -> None:
        if not self.__has_as_clone(clone):
            raise InvalidInputException(f"{self} doesn't have {clone} as clone. ")
        self.logger.info(f"Deactivating clone worker thread {self.__clones[clone].getName()}")
        self.__clones_must_quit[clone].set()
        self.__clones_are_active[clone].set()     # Activate the thread such that it can quit
        self.logger.info(f"Waiting for clone worker thread to join...")
        self.__clones[clone].join()
        self.logger.info(f"Clone worker thread joined")
        clone.undeploy()    # TODO may raise exceptions
        self.logger.info(f"Clone got undeployed")
        del self.__clones_are_active[clone]
        del self.__clones_must_quit[clone]
        del self.__clones[clone]
        clone.delete()

    def __can_have_as_clone(self, clone: ICloneCell) -> "BoolExplained":
        if not isinstance(clone, ICloneCell):
            return FalseExplained(f"{self} expected a clone to be an instance of ICloneCell, got {type(clone)}")
        return TrueExplained()

    def __has_as_clone(self, clone: ICloneCell) -> bool:
        return clone in self.__clones.keys()

    def __get_nb_clones(self) -> int:
        return len(self.__clones)

    def get_nb_clones_by_type(self) -> Dict[Type[ICloneCell], int]:
        nb_clones_by_type: Dict[Type[ICloneCell], int] = {}
        for clone in self.get_all_clones():
            nb_clones_by_type[type(clone)] = nb_clones_by_type.get(type(clone), 0) + 1
        return nb_clones_by_type

    def __assert_has_proper_clones(self) -> None:
        # the clones must be consistent with the configured clone types.
        scalable_cell = self.get_scalable_cell_deployment().get_scalable_cell()
        if self.get_nb_clones_by_type() != scalable_cell.get_nb_clone_types_by_type():
            raise InvalidStateException(f"The clones of {self} don't correspond with the configured clone types "
                                        f"of its scalable cell {scalable_cell}.")

        # the clones used as keys in these two dictionaries must be the same    # TODO extend
        if set(self.__clones.keys()) != set(self.__clones_are_active.keys()):
            raise InvalidStateException(f"{self}: internal dict keys don't match: {set(self.__clones.keys())} and "
                                        f"{set(self.__clones_are_active.keys())}")

        # can_have_as_clone must be true for all clones
        for clone in self.get_all_clones():
            raise_if_not(self.__can_have_as_clone(clone), InvalidStateException)

        # Also check the full validity of the clone clones
        for clone in self.get_all_clones():
            clone.assert_is_valid()

    # ------ Pulling ------

    def _on_pull(self) -> None:
        if not self.__has_started:
            self.start()
        # The cells are pulling continuously in their worker threads
        return

    def pause(self) -> None:
        nb_clones = len(self.__clones_are_active)
        for i, (clone, clone_is_active) in enumerate(self.__clones_are_active.items()):
            self.logger.info(f"{self} pausing {i+1}/{nb_clones}...")
            if clone_is_active.is_set():
                self.logger.warning(f"{self}: pausing a clone while it is still active, "    # TODO
                                    f"is not threadsafe yet (-> might block forever).")
            clone_is_active.clear()
            self.__clones_have_paused[clone].wait()
            self.logger.info(f"{self} pausing {i+1}/{nb_clones} done")
        self.__has_started = False

    def reset(self) -> None:
        self.pause()
        for clone in self.get_all_clones():
            clone.reset()

    def start(self) -> None:
        for _, clone_is_active in self.__clones_are_active.items():
            clone_is_active.set()
        self.__has_started = True

    def is_active(self) -> bool:
        if not self.__has_started:
            return False
        if all([has_paused.is_set() for has_paused in self.__clones_have_paused.values()]):
            self.__has_started = False
            return False
        return True

    # ------ Validation ------

    def assert_is_valid(self) -> None:
        super(CloningStrategy, self).assert_is_valid()
        self.__assert_has_proper_clones()

    # ------ Deletion ------

    def delete(self) -> None:
        for clone_type, amount in self.get_nb_clones_by_type().items():
            for i in range(amount):
                self.logger.info(f"{self} removing {i+1}/{amount}...")
                self.remove_clone(clone_type)
                self.logger.info(f"{self} removing {i+1}/{amount} done")
        assert self.__get_nb_clones() == 0
        super(CloningStrategy, self).delete()
