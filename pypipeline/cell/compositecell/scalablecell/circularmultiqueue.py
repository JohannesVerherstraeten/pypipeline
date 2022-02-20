# Copyright 2021 Johannes Verherstraeten
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, Dict, List, Generic, TypeVar, Iterable, Hashable, Tuple
from threading import BoundedSemaphore, Lock
from pprint import pformat
import logging

from pypipeline.exceptions import TimeoutException


Q = TypeVar("Q", bound=Hashable)
T = TypeVar("T")


class CircularMultiQueue(Generic[Q, T]):
    """
    Circular multi-queue with a ticketing system (~queue index).

    Every lane of the multi-queue is associated with a specific ID (type Q). The values of the queue are of type T.

    You can acquire a ticket (queue index), which will block as long as there is no room in the queue.
    This reserves a specific spot in the queue.
    Then you can place elements on every lane at this queue index, at any time you want.
    Another thread can wait_and_dequeue. This will always return the queue elements in the ordering that the tickets
    were acquired. If the elements at the next queue index are not yet available, it will block.
    """

    def __init__(self, queue_ids: Iterable[Q], capacity: int = 2):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__capacity = capacity
        self.__queues: Dict[Q, List[Optional[T]]] = {queue_id: [None for _ in range(self.__capacity)]
                                                     for queue_id in queue_ids}
        self.__exceptions: List[Optional[Exception]] = [None for _ in range(self.__capacity)]
        self.__reservation_index = 0            # tail
        self.__next_result_index = 0            # head
        self.__end_index: Optional[int] = None
        self.__reservation_semaphore = BoundedSemaphore(self.__capacity)
        self.__result_ready_semaphores: List[BoundedSemaphore] = [BoundedSemaphore(1) for _ in range(self.__capacity)]
        self.__queue_index_lock = Lock()
        self.__output_lock: Lock = Lock()

        # set all to 0
        for semaphore in self.__result_ready_semaphores:
            semaphore.acquire()

    def get_capacity(self) -> int:
        """
        Returns:
            The queue capacity.
        """
        return self.__capacity

    def get_queue_ids(self) -> Tuple[Q, ...]:
        """
        Returns:
            The IDs of the queue lanes of this multi-queue.
        """
        return tuple(self.__queues.keys())

    def acquire_queue_index(self, timeout: Optional[float] = None) -> int:
        """
        Claims and returns the next queue index that is still free and not yet claimed. This call blocks as long as no
        such queue index is available.

        Args:
            timeout: an optional timeout for the blocked waiting.
        Returns:
            The next free queue index, which is now owned by the caller of this method.
        Raises:
            TimeoutException: when acquiring the new queue index times out.
        """
        with self.__queue_index_lock:
            # Wait until there is room in the queues
            if not self.__reservation_semaphore.acquire(timeout=timeout):
                raise TimeoutException(f"Waiting for room in the queue timed out.")
            queue_index = self.__reservation_index
            self.__reservation_index = (self.__reservation_index + 1) % self.__capacity
            return queue_index

    def set(self, queue_id: Q, queue_index: int, element: T) -> None:
        """
        Set a value (element) at the given queue index, in the queue lane with the given ID.

        Should only be called by the owner of the queue index (requested via acquire_queue_index).

        Args:
            queue_id: the ID of the queue lane to put the element in.
            queue_index: the index in the queue lane where the element must be put.
            element: the element to put in the queue.
        """
        self.__queues[queue_id][queue_index] = element

    def set_exception(self, queue_index: int, exception: Exception) -> None:
        """
        Set an exception instance at the given queue index. This exception will be raised when the item at that
        queue index is popped.

        Should only be called by the owner of the queue index (requested via acquire_queue_index).

        Args:
            queue_index: the index in the queue lane where the element must be put.
            exception: the exception to put in the queue.
        """
        self.__exceptions[queue_index] = exception

    def signal_queue_index_ready(self, queue_index: int) -> None:
        """
        Indicate that all queue lanes at the given queue index are set with a value, and signal any thread that's
        waiting for this index.

        Should only be called by the owner of the queue index (requested via acquire_queue_index).

        Args:
            queue_index: the queue index that is ready.
        """
        self.logger.debug(f"Signalling index {queue_index} is ready!")
        self.__result_ready_semaphores[queue_index].release()

    def wait_and_pop(self, timeout: Optional[float] = None) -> Dict[Q, T]:
        """
        Pops one item from every queue lane at the next queue index. This method blocks as long as this queue index
        has not been signalled to be ready.

        Args:
            timeout: an optional timeout for the blocked waiting.
        Returns:
            The items from the next queue index, ordered by queue ID.
        Raises:
            TimeoutException: when waiting for the next queue index times out.
            StopIteration: when the end of the queue is reached.
        """
        with self.__output_lock:
            # Wait for next result to be ready
            self.__wait_for_next_result(timeout)

            # If we got signalled that the next result is ready, first check whether it's an exception
            queued_exception = self.__exceptions[self.__next_result_index]
            if queued_exception is not None:
                if not isinstance(queued_exception, StopIteration):
                    self.__exceptions[self.__next_result_index] = None
                    self.__next_result_index = (self.__next_result_index + 1) % self.__capacity
                self.__reservation_semaphore.release()
                raise queued_exception
            else:
                result: Dict[Q, T] = {}
                for queue_id, queue in self.__queues.items():
                    # At this moment we know the item at the next index is available, so it is of type T.
                    # We could do an `assert queue_item is not None`, however this would cause problems when T
                    # would be an Optional type as well. Therefore we omit the assert and place a type_ignore to calm
                    # mypy.
                    queue_item: T = queue[self.__next_result_index]     # type: ignore
                    result[queue_id] = queue_item

                # The next pop should get the result from the the next index in the queues
                self.__next_result_index = (self.__next_result_index + 1) % self.__capacity
                self.__reservation_semaphore.release()
                return result

    def __wait_for_next_result(self, timeout: Optional[float]) -> None:
        # Wait here until the next result in the result queue is ready.
        next_result_is_ready: BoundedSemaphore = self.__result_ready_semaphores[self.__next_result_index]
        self.logger.debug(f"Waiting for index {self.__next_result_index}...")
        acquired = next_result_is_ready.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutException(f"Waiting for queue values at index {self.__next_result_index} timed out.")

    def reset(self) -> None:
        """
        Hard reset the queue.
        """
        self.__reservation_index = 0
        self.__next_result_index = 0
        self.__end_index = None
        self.__result_ready_semaphores = [BoundedSemaphore(1) for _ in range(self.__capacity)]
        [semaphore.acquire() for semaphore in self.__result_ready_semaphores]  # set all to 0
        self.__reservation_semaphore = BoundedSemaphore(self.__capacity)
        self.__exceptions: List[Optional[Exception]] = [None for _ in range(self.__capacity)]

    def __getstate__(self) -> Dict:
        # called during pickling
        raise Exception(f"CircularMultiQueue objects cannot be pickled")

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: \n" \
               f"Queues: \n{pformat({str(key): value for key, value in self.__queues.items()})} \n" \
               f"Reservation index: {self.__reservation_index}\n" \
               f"Next result index: {self.__next_result_index}\n" \
               f"End index: {self.__end_index}"
