"""
Tests some edge cases of `.extract_keywords`.
"""

import pytest
from pyrseus.ctx.api import extract_keywords

#
# Call signature types that we support.
#


def test_max_workers_positional_only_okay():
    def factory(max_workers, /, foo=1, bar=2):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


def test_max_workers_dual_use_okay():
    def factory(max_workers, *, foo=1, bar=2):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


def test_unrestricted_okay():
    def factory(max_workers, foo=1, bar=2):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


def test_max_workers_with_default_okay():
    def factory(max_workers=0, foo=1, bar=2):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


def test_extra_dual_use_with_default_okay():
    def factory(max_workers, /, foo=1, *, bar=2):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


def test_self_okay():
    def factory(self, max_workers, /, foo=1, *, bar=1):
        pass

    assert extract_keywords(factory) == {"foo", "bar"}


class BadBase:
    # Invalid call signature in this base class, but it's okay because one of
    # our subclasses blocks the recursion by not taking a **kwargs arg.
    def __init__(self, **kwargs):
        pass


class BlockingSub(BadBase):
    # This subclass' lack of a **kwargs arg stops BadBase from breaking the
    # test.
    def __init__(self, max_workers, a=1, b=2):
        pass


class KwSub(BlockingSub):
    # An intermediate subclass that takes a **kwargs arg.
    def __init__(self, max_workers, b=3, c=4, **kwargs):
        pass


class Concrete(KwSub):
    # A final subclass that takes a **kwargs arg.
    def __init__(self, max_workers, *, d=5, **kwargs):
        pass


def test_complex_inheritance():
    assert extract_keywords(Concrete) == {"a", "b", "c", "d"}

    with pytest.raises(TypeError, match="first .* max_workers"):
        extract_keywords(BadBase)


#
# Call signature types that we do *not* support.
#


def test_extra_dual_use_non_default_fails():
    def factory(max_workers, /, foo, *, bar=1):
        pass

    with pytest.raises(TypeError, match="Parameter 'foo' has no default."):
        assert extract_keywords(factory) == {"foo", "bar"}


def test_max_workers_kw_only_fails():
    def factory(*, max_workers=1, foo=1, bar=2):
        pass

    # For now, we require that max_workers can be supplied positionally.
    with pytest.raises(TypeError, match="must be positional .* max_workers"):
        extract_keywords(factory)


def test_extra_positional_fails():
    def factory(max_workers, foo, /, *, bar=1):
        pass

    # No extra positional-only arguments are allowed.
    with pytest.raises(TypeError, match="The only allowed positional-only"):
        extract_keywords(factory)
