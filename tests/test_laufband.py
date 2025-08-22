"""Test for Laufband length behavior with different data types."""

import pytest

from laufband import Laufband


def test_laufband_length_with_list(tmp_path):
    """Test that Laufband with a list supports len() operation."""
    data = list(range(10))
    laufband = Laufband(data, db=f"sqlite:///{tmp_path}/laufband.sqlite")

    assert len(laufband) == 10
    assert len(list(laufband)) == 10


def test_laufband_length_with_generator_raises_error(tmp_path):
    """Test that Laufband with a generator raises TypeError for len() operation."""
    data = (x for x in range(10))  # Generator expression
    laufband = Laufband(data, db=f"sqlite:///{tmp_path}/laufband.sqlite")

    # Should raise TypeError when trying to get length from a generator
    with pytest.raises(TypeError, match="object of type 'generator' has no len"):
        len(laufband)

    # But iteration should still work correctly
    results = list(laufband)
    assert len(results) == 10
    assert set(results) == set(range(10))

