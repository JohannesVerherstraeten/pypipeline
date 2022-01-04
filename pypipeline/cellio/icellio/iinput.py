# Copyright (C) 2021  Johannes Verherstraeten
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see https://www.gnu.org/licenses/agpl-3.0.en.html

from typing import TypeVar, Generic
from abc import ABC, abstractmethod

from pypipeline.cellio.icellio.io import IO


T = TypeVar('T')


class IInput(IO[T], ABC, Generic[T]):

    @abstractmethod
    def is_provided(self) -> bool:
        """
        Returns:
            True if this input is provided by a default value / incoming connection or another way of value provision.
        """
        raise NotImplementedError
