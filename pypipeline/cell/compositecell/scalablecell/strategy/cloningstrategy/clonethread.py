from typing import Optional, TYPE_CHECKING, Any, Collection, Iterator
from threading import Thread, Lock, Event
from contextlib import contextmanager
import logging
from time import time
from prometheus_client import Histogram

import pypipeline
from pypipeline.cell.compositecell.scalablecell.strategy.cloningstrategy.clonecell import ICloneCell
from pypipeline.exceptions import TimeoutException, NonCriticalException

if TYPE_CHECKING:
    from pypipeline.cell.compositecell.scalablecell.circularmultiqueue import CircularMultiQueue
    from pypipeline.cellio.compositeio import InputPort, OutputPort


class CloneThread(Thread):
    """
    The clone thread class holds all functionality and state of one thread inside a scalable cell.

    A scalable cell manages each of its clones in a separate thread (even Ray clones). This thread pulls the
    inputs of the scalable cell, pass the inputs to the clones and put the clone results in the scalable
    cell's output queue.
    """

    def __init__(self,
                 clone: ICloneCell,
                 scalable_cell_input_ports: Collection["InputPort"],
                 scalable_cell_output_ports: Collection["OutputPort"],
                 is_active: Event,
                 must_quit: Event,
                 has_paused: Event,
                 queue_reservation_lock: Lock,
                 output_queue: "CircularMultiQueue[OutputPort, Any]",
                 pull_duration_metric: Histogram,
                 check_quit_interval: float = 5.,
                 thread_name: Optional[str] = None):
        super(CloneThread, self).__init__(name=thread_name)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.clone = clone
        self.scalable_cell_input_ports = scalable_cell_input_ports
        self.scalable_cell_output_ports = scalable_cell_output_ports
        self.is_active = is_active      # can be set by both thread manager and thread itself
        self.has_paused = has_paused    # should only be set by thread itself (this)
        self.must_quit = must_quit      # should only be set by thread manager
        self.queue_reservation_lock = queue_reservation_lock
        self.output_queue = output_queue
        self.check_quit_interval = check_quit_interval
        self.got_stopiteration = False
        self.exception: Optional[Exception] = None
        self.pull_duration_metric = pull_duration_metric

    def run(self) -> None:
        """
        The thread's mainloop.
        """
        self.logger.info(f"clone worker started")
        while self.is_active.wait():
            self.has_paused.clear()
            self.logger.debug(f"flag1")
            # Require a queue index reservation lock, to make sure no 2 clone workers reserve the same
            # place in the output queues. The thread having this lock may acquire a new spot in
            # the queue and pull the inputs. (critical section)
            with self._acquire_queue_reservation_lock() as acquired:
                self.logger.debug(f"flag2")
                if not acquired:
                    # The thread has been signalled to quit
                    break
                self.logger.debug(f"flag3")

                # Wait for a free spot in the output queues
                queue_idx = self._acquire_queue_index()
                self.logger.debug(f"flag4")
                if queue_idx is None:
                    # The thread has been signalled to quit
                    break
                self.logger.debug(f"flag5")

                # Pull the inputs. This should happen inside the queue index reservation lock,
                # such that no 2 clone workers could pull the inputs at the same time.
                self._pull_inputs(queue_idx)
                self.logger.debug(f"flag6")

            # If we didn't become inactive because of one source cells got exhausted, execute the clone.
            if self.is_active.is_set():
                t0 = time()
                self._pull_clone()
                t1 = time()
                self.pull_duration_metric.observe(t1 - t0)
                self.logger.debug(f"flag7")
            else:
                self.logger.debug(f"flag7.1")

            # Check if we didn't become inactive because of an exception.
            if self.is_active.is_set():
                self._set_outputs(queue_idx)
                self.logger.debug(f"flag8")
            else:
                assert self.exception is not None, "Got inactive without any exception being registered"
                self.logger.info(f"Queueing up exception: {type(self.exception)}{str(self.exception)}")
                self.output_queue.set_exception(queue_idx, self.exception)

                if isinstance(self.exception, NonCriticalException):
                    self.logger.info(f"Resetting clone {self.clone} after exception")
                    self.clone.reset()
                    self.is_active.set()
                self.logger.debug(f"flag8.1")

            self.output_queue.signal_queue_index_ready(queue_idx)

            self.logger.debug(f"flag9")
            if self.must_quit.is_set():
                break
            if not self.is_active.is_set():
                self.has_paused.set()
        self.logger.info("worker finished")

    @contextmanager
    def _acquire_queue_reservation_lock(self) -> Iterator[bool]:
        """Returns True if acquired, False if the clone must quit. If both, quitting has precedence. """
        acquired = False
        while not self.must_quit.is_set() and not acquired:
            acquired = self.queue_reservation_lock.acquire(timeout=self.check_quit_interval)
            if not acquired:
                self.logger.info(f"flag1.1: waiting for the queue reservation lock...")

        # At this moment the thread acquired the queue reservation lock AND/OR the thread must quit
        if acquired:
            try:
                yield not self.must_quit.is_set()
            finally:
                self.queue_reservation_lock.release()
        else:
            yield False

    def _acquire_queue_index(self) -> Optional[int]:
        """
        Returns the acquired queue index if acquired, or None if the clone must quit.
        If both, the acquired queue index has precedence.
        """
        queue_index: Optional[int] = None
        while not self.must_quit.is_set() and queue_index is None:
            try:
                queue_index = self.output_queue.acquire_queue_index(timeout=self.check_quit_interval)       # TODO may raise exceptions
            except TimeoutException:
                self.logger.info(f"flag3.1: waiting for room in the output queue...")

        # At this moment the thread acquired a queue index (=queue index is not None) AND/OR the thread must quit
        return queue_index

    def _pull_inputs(self, queue_index: int) -> None:
        clone_inputs = set(self.clone.get_clone_inputs())
        # Pull the input ports
        for scalable_cell_input in self.scalable_cell_input_ports:
            self.logger.debug(f"Pulling {str(scalable_cell_input)}")
            try:
                # Only raises an exception if any of the upstream cells raises.
                input_value: Any = scalable_cell_input.pull()
            except Exception as e:
                self.logger.info(f"{self} caught following exception and therefore pauses: {type(e)}: {str(e)}")
                self.exception = e
                self.is_active.clear()
                return
            self.logger.debug(f"Pulling {str(scalable_cell_input)} done")
            # Set the inputs of the internal cells
            scalable_cell_input_connections = scalable_cell_input.get_outgoing_connections()
            self.logger.debug(f"Setting clone inputs... (#{len(scalable_cell_input_connections)})")
            for scalable_cell_input_connection in scalable_cell_input_connections:
                connection_target = scalable_cell_input_connection.get_target()
                self.logger.debug(f"{self} setting input value from {scalable_cell_input} at {connection_target}...")
                if connection_target in self.scalable_cell_output_ports:
                    # Internal connection directly from InputPort to OutputPort
                    # So connection_target is an OutputPort (=queue_id)
                    assert isinstance(connection_target, pypipeline.cellio.OutputPort)
                    self.output_queue.set(connection_target, queue_index, input_value)
                else:
                    # Internal connection from InputPort to the internal cell
                    internal_cell_input_name = scalable_cell_input_connection.get_target().get_name()
                    clone_input = self.clone.get_clone_input(internal_cell_input_name)  # should not raise KeyError
                    clone_input.set_value(input_value)  # should not raise NotDeployedException or InvalidInputException
                    clone_inputs.remove(clone_input)
                self.logger.debug(f"{self} setting input value from {scalable_cell_input} at {connection_target} done")
        # Provide the clone_inputs that have no incoming connection from an InputPort with None
        for clone_input in clone_inputs:
            clone_input.set_value(None)

    def _pull_clone(self) -> None:
        try:
            self.clone.pull()
        except Exception as e:
            self.exception = e
            self.is_active.clear()

    def _set_outputs(self, queue_index: int) -> None:
        cell_and_clone_outputs = zip(self.scalable_cell_output_ports, self.clone.get_clone_outputs())
        for scalable_cell_output, clone_output in cell_and_clone_outputs:
            output_value = clone_output.pull()
            self.output_queue.set(scalable_cell_output, queue_index, output_value)
