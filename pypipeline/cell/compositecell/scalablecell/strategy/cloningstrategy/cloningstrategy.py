from typing import Optional, Dict, Sequence, TYPE_CHECKING, Type
from threading import Thread, Event, Lock

from pypipeline.cell.compositecell.scalablecell.strategy.ascalingstrategy import AScalingStrategy
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonethread import CloneThread
from pypipeline.exceptions import NoInternalCellException, InvalidStateException

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.scalablecell.scalablecelldeployment import ScalableCellDeployment


class CloningStrategy(AScalingStrategy):

    def __init__(self, scalable_cell_deployment: "ScalableCellDeployment"):
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

    def get_queue_reservation_lock(self) -> Lock:
        return self.__queue_reservation_lock

    # ------ Clones ------

    def get_all_clones(self) -> Sequence[ICloneCell]:
        return tuple(self.__clones.keys())

    def add_clone(self, method: Type[ICloneCell]) -> None:
        internal_cell = self.get_internal_cell()
        if internal_cell is None:
            raise NoInternalCellException(f"{self.get_scalable_cell_deployment().get_scalable_cell()} "
                                          f"has no internal cell that can be scaled up. ")
        scalable_cell_name = self.get_scalable_cell_deployment().get_scalable_cell().get_full_name()
        scalable_cell_name = str(scalable_cell_name).replace(".", "-")
        clone_name = f"{scalable_cell_name} - worker{self.__worker_id_counter}"
        clone = method.create(internal_cell, clone_name)    # TODO may raise exceptions
        self.logger.info(f"{self} created clone: {clone}")
        self.__add_clone(clone)

    def __add_clone(self, clone: ICloneCell) -> None:
        assert self.__can_have_as_clone(clone)
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
                                   self.get_queue_reservation_lock(),
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
        assert self.__has_as_clone(clone)
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

    def __can_have_as_clone(self, clone: ICloneCell) -> bool:
        return isinstance(clone, ICloneCell)

    def __has_as_clone(self, clone: ICloneCell) -> bool:
        return clone in self.__clones.keys()

    def __get_nb_clones(self) -> int:
        return len(self.__clones)

    def get_nb_clones_by_type(self) -> Dict[Type[ICloneCell], int]:
        nb_clones_by_type: Dict[Type[ICloneCell], int] = {}
        for clone in self.get_all_clones():
            nb_clones_by_type[type(clone)] = nb_clones_by_type.get(type(clone), 0) + 1
        return nb_clones_by_type

    def __has_proper_clones(self) -> bool:
        # the clones must be consistent with the configured clone types.
        if self.get_nb_clones_by_type() != \
                self.get_scalable_cell_deployment().get_scalable_cell().get_nb_clone_types_by_type():
            return False
        # can_have_as_clone must be true for all clones
        for clone in self.get_all_clones():
            if not self.__can_have_as_clone(clone):
                return False

        # the clones used as keys in these two dictionaries must be the same    # TODO extend
        if set(self.__clones.keys()) != set(self.__clones_are_active.keys()):
            return False

        return True

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
        if not self.__has_proper_clones():
            raise InvalidStateException()   # TODO

        # Also check the full validity of the clone clones
        for clone in self.get_all_clones():
            clone.assert_is_valid()

    # ------ Deletion ------

    def delete(self) -> None:
        for clone_type, amount in self.get_nb_clones_by_type().items():
            for i in range(amount):
                self.logger.info(f"{self} removing {i+1}/{amount}...")
                self.remove_clone(clone_type)
                self.logger.info(f"{self} removing {i+1}/{amount} done")
        assert self.__get_nb_clones() == 0
        super(CloningStrategy, self).delete()
