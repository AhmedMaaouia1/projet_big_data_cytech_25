import pytest
from src.utils import iter_months_inclusive


def test_iter_months_inclusive_same_month():
    months = iter_months_inclusive(2023, 1, 2023, 1)
    assert [(m.year, m.month) for m in months] == [(2023, 1)]


def test_iter_months_inclusive_across_months():
    months = iter_months_inclusive(2023, 1, 2023, 3)
    assert [(m.year, m.month) for m in months] == [(2023, 1), (2023, 2), (2023, 3)]


def test_iter_months_inclusive_invalid_range():
    with pytest.raises(ValueError):
        iter_months_inclusive(2023, 5, 2023, 4)
