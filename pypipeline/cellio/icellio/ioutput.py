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

from typing import TypeVar, Generic

from pypipeline.cellio.icellio.io import IO


T = TypeVar('T')


class IOutput(IO[T], Generic[T]):

    def set_value(self, value: T) -> None:
        """
        Provide a new value for this IO.

        The public version of IO._set_value(value)

        Args:
            value: the new value for this IO.
        Raises:
            TODO may raise exceptions
        """
        raise NotImplementedError

    def all_outgoing_connections_have_pulled(self) -> bool:
        """
        Returns:
            True if all outgoing connections have pulled, False otherwise.
        """
        raise NotImplementedError
