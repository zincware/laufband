"""Test for Laufband length behavior with different data types."""

import pytest

from laufband import Laufband


def test_laufband_length_with_list(tmp_path):
    """Test that Laufband with a list supports len() operation."""
    data = list(range(10))
    db_path = tmp_path / "length_test_list.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should be able to get length from a list
    assert len(laufband) == 10

    # Verify the data is accessible and correct
    results = list(laufband)
    assert len(results) == 10
    assert set(results) == set(range(10))


def test_laufband_length_with_generator_raises_error(tmp_path):
    """Test that Laufband with a generator raises TypeError for len() operation."""
    data = (x for x in range(10))  # Generator expression
    db_path = tmp_path / "length_test_generator.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should raise TypeError when trying to get length from a generator
    with pytest.raises(TypeError, match="object of type 'generator' has no len"):
        len(laufband)

    # But iteration should still work correctly
    results = list(laufband)
    assert len(results) == 10
    assert set(results) == set(range(10))


def test_laufband_length_with_tuple(tmp_path):
    """Test that Laufband with a tuple supports len() operation."""
    data = tuple(range(5))
    db_path = tmp_path / "length_test_tuple.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should be able to get length from a tuple
    assert len(laufband) == 5

    # Verify the data is accessible and correct
    results = list(laufband)
    assert len(results) == 5
    assert set(results) == set(range(5))


def test_laufband_length_with_range(tmp_path):
    """Test that Laufband with a range supports len() operation."""
    data = range(7)
    db_path = tmp_path / "length_test_range.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should be able to get length from a range
    assert len(laufband) == 7

    # Verify the data is accessible and correct
    results = list(laufband)
    assert len(results) == 7
    assert set(results) == set(range(7))


def test_laufband_length_with_custom_iterable_no_len(tmp_path):
    """Test that Laufband with a custom iterable without __len__ raises TypeError."""

    class CustomIterable:
        """Custom iterable without __len__ method."""

        def __init__(self, data):
            self.data = data

        def __iter__(self):
            return iter(self.data)

    data = CustomIterable([1, 2, 3, 4])
    db_path = tmp_path / "length_test_custom.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should raise TypeError when trying to get length from custom iterable without __len__
    with pytest.raises(TypeError):
        len(laufband)

    # But iteration should still work correctly
    results = list(laufband)
    assert len(results) == 4
    assert set(results) == {1, 2, 3, 4}


def test_laufband_length_with_custom_iterable_with_len(tmp_path):
    """Test that Laufband with a custom iterable with __len__ supports len() operation."""

    class CustomIterableWithLen:
        """Custom iterable with __len__ method."""

        def __init__(self, data):
            self.data = data

        def __iter__(self):
            return iter(self.data)

        def __len__(self):
            return len(self.data)

    data = CustomIterableWithLen([10, 20, 30])
    db_path = tmp_path / "length_test_custom_len.sqlite"

    laufband = Laufband(data, com=db_path, cleanup=False)

    # Should be able to get length from custom iterable with __len__
    assert len(laufband) == 3

    # Verify the data is accessible and correct
    results = list(laufband)
    assert len(results) == 3
    assert set(results) == {10, 20, 30}
