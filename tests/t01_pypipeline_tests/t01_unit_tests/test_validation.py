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

import pytest

from pypipeline.validation import BoolExplained, TrueExplained, FalseExplained, raise_if_not, assert_


def test_creation_abstract_class() -> None:
    with pytest.raises(TypeError):
        BoolExplained()


def test_creation_true() -> None:
    val = TrueExplained()
    assert val.value
    assert bool(val)
    assert val
    assert len(val.reasons) == 0


def test_creation_false_one_no_reasons() -> None:
    with pytest.raises(ValueError):
        FalseExplained([])


def test_creation_false_one_reason() -> None:
    val = FalseExplained("a reason")
    assert not val.value
    assert not bool(val)
    assert not val
    assert "a reason" in val.reasons


def test_creation_false_multiple_reasons() -> None:
    val = FalseExplained(["reason one", "reason two"])
    assert not val.value
    assert not bool(val)
    assert not val
    assert len(val.reasons) == 2


def test_get_reasons_str_no_reason() -> None:
    val = TrueExplained()
    assert len(val.get_reasons_str(pretty=False)) == 0
    assert len(val.get_reasons_str(pretty=True)) == 0


def test_get_reasons_str_one_reason() -> None:
    val = FalseExplained("a reason")
    assert val.get_reasons_str(pretty=False) == "a reason"
    assert val.get_reasons_str(pretty=True) == "a reason"


def test_get_reasons_str_multiple_reasons() -> None:
    val = FalseExplained(["reason one", "reason_two"])
    assert val.get_reasons_str(pretty=False) == str(["reason one", "reason_two"])
    assert val.get_reasons_str(pretty=True) == "\n - reason one\n - reason_two"


def test_str_true() -> None:
    val = TrueExplained()
    assert str(val) == "True"


def test_str_false() -> None:
    val = FalseExplained("a reason")
    assert str(val) == "False(reasons: a reason)"


def test_pretty_no_reason() -> None:
    val = TrueExplained()
    assert val.pretty() == "True"


def test_pretty_one_reason() -> None:
    val = FalseExplained("a reason")
    assert val.pretty() == f"False(reasons: a reason)"


def test_pretty_multiple_reasons() -> None:
    val = FalseExplained(["reason one", "reason two"])
    assert val.pretty() == f"False(reasons: \n - reason one\n - reason two\n)"


def test_mul_false_false() -> None:
    val1 = FalseExplained("a reason")
    val2 = FalseExplained(["reason one", "reason two"])
    result = val1 * val2
    assert isinstance(result, BoolExplained)
    assert not result
    assert len(result.reasons) == 3


def test_mul_true_false() -> None:
    val1 = TrueExplained()
    val2 = FalseExplained(["reason one", "reason two"])
    result = val1 * val2
    assert isinstance(result, BoolExplained)
    assert not result
    assert len(result.reasons) == 2


def test_mul_false_true() -> None:
    val1 = TrueExplained()
    val2 = FalseExplained(["reason one", "reason two"])
    result = val2 * val1
    assert isinstance(result, BoolExplained)
    assert not result
    assert len(result.reasons) == 2


def test_mul_true_true() -> None:
    val1 = TrueExplained()
    val2 = TrueExplained()
    result = val2 * val1
    assert isinstance(result, BoolExplained)
    assert result


def test_mul_true_real_true() -> None:
    val1 = TrueExplained()
    result = val1 * True
    assert isinstance(result, BoolExplained)
    assert result


def test_mul_false_real_true() -> None:
    val1 = FalseExplained("a reason")
    result = val1 * True
    assert isinstance(result, BoolExplained)
    assert not result
    assert len(result.reasons) == 1


def test_mul_false_real_false() -> None:
    val1 = FalseExplained("a reason")
    with pytest.raises(ValueError):
        val1 * False


def test_raise_if_not_with_true() -> None:
    raise_if_not(TrueExplained(), AssertionError)


def test_raise_if_not_with_false() -> None:
    with pytest.raises(AssertionError):
        raise_if_not(FalseExplained("a reason"), AssertionError)


def test_assert_with_true() -> None:
    assert_(lambda: TrueExplained())


def test_assert_with_false() -> None:
    with pytest.raises(AssertionError):
        assert_(lambda: FalseExplained("a reason"))
