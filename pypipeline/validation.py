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

from typing import List, Union, Type, Callable
from abc import ABC, abstractmethod


class BoolExplained(ABC):
    """
    Boolean including one or more reasons if False.

    Doesn't have a reason if True.
    Can be used just like a normal bool.
    """

    def __bool__(self) -> bool:
        return self.value

    @property
    @abstractmethod
    def value(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def reasons(self) -> List[str]:
        raise NotImplementedError

    def get_reasons_str(self, pretty: bool = False) -> str:
        reasons = self.reasons
        if len(reasons) == 0:
            return ""
        elif len(reasons) == 1:
            return reasons[0]
        elif pretty:
            reasons_str = ""
            for reason in reasons:
                reasons_str += f"\n - {reason}"
            return reasons_str
        else:
            return str(reasons)

    def __str__(self) -> str:
        return f"{bool(self)}" + (f"(reasons: {self.get_reasons_str()})" if not self else "")

    def pretty(self) -> str:
        reasons = self.reasons
        if bool(self):
            return "True"
        assert len(reasons) > 0
        if len(reasons) == 1:
            return f"False(reasons: {self.get_reasons_str(pretty=True)})"
        else:
            result = f"False(reasons: "
            result += self.get_reasons_str(pretty=True)
            result += "\n)"
            return result

    def __mul__(self, other: object) -> "BoolExplained":
        if isinstance(other, BoolExplained):
            if bool(self) and bool(other):
                return TrueExplained()
            else:
                return FalseExplained(self.reasons + other.reasons)
        elif other:
            return self
        else:
            raise ValueError(f"No reason given why `other` is False")


class TrueExplained(BoolExplained):

    def __init__(self) -> None:
        pass

    @property
    def value(self) -> bool:
        return True

    @property
    def reasons(self) -> List[str]:
        return []


class FalseExplained(BoolExplained):

    def __init__(self, reasons: Union[str, List[str]]) -> None:
        if isinstance(reasons, str):
            reasons = [reasons]
        else:
            if len(reasons) == 0:
                raise ValueError(f"FalseExplained created without giving a reason")
        self.__reasons = reasons

    @property
    def value(self) -> bool:
        return False

    @property
    def reasons(self) -> List[str]:
        return self.__reasons


def raise_if_not(bool_with_explanation: BoolExplained,
                 exception_type: Type[Exception],
                 message_prefix: str = "") -> None:
    """
    Raises an exception if the given BoolExplained instance is False.

    The message of the exception will start with the given (optional) message prefix, followed by the
    reason of the FalseExplained.
    """
    if not bool_with_explanation:
        raise exception_type(message_prefix + bool_with_explanation.get_reasons_str(pretty=True))


def assert_(validation_fn: Callable[[], BoolExplained],
            message_prefix: str = "") -> None:
    """
    Asserts that the given validation function evaluates to a TrueExplained.

    The message of the assertion will start with the given (optional) message prefix, followed by the
    reason of the FalseExplained.
    """
    assert validation_fn(), message_prefix + validation_fn().get_reasons_str(pretty=True)
