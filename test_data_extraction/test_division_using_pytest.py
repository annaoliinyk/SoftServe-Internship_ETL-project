import pytest

from test_data_extraction.divide_for_testing import Calculator


def test_raises_zero_division():
    with pytest.raises(ZeroDivisionError):
        Calculator(3, 0).division()


def test_division():
    assert Calculator(0, 3).division() == 0
