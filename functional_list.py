from collections import defaultdict
from typing import Callable, Iterable, Any, Tuple, Union, Dict, List

pattern_type = Union[None, str, Tuple["Pattern"], ...]


class FLMetaClass(type):

    def __getitem__(self, *args):
        return FL(*args)


class FL(list, metaclass=FLMetaClass):

    def map(self, mapper: Callable[[Any], Any]) -> "FL":
        return FL(mapper(value) for value in self)

    def named_map(self, pattern: pattern_type, mapper: Callable[[Any, ...], Any]):
        indexes = _get_indexes(pattern)
        named_values = [_get_named_values(indexes, obj) for obj in self]
        return FL(mapper(**values) for values in named_values)

    def flatten(self) -> "FL":
        def iter_flatten():
            for iterable in self:
                for value in iterable:
                    yield value

        return FL(iter_flatten())

    def flat_map(self, mapper: Callable[[Any], Iterable[Any]]) -> "FL":
        return self.map(mapper).flatten()

    def reduce(self, reducer: Callable[[Any, Any], Any]) -> Any:
        if len(self) == 0:
            return None
        iterator = iter(self)
        value = next(iterator)
        for next_value in iterator:
            value = reducer(value, next_value)
        return value

    def reduce_by_key(self, reducer: Callable[[Any, Any], Any]) -> "FL":
        key_values = FL(self._group_by_key().items())
        return key_values.map(lambda e: (e[0], e[1].reduce(reducer)))

    def join_by_key(self, other: "FL") -> "FL":
        self_grouped = self._group_by_key()
        other_grouped = other._group_by_key()

        def iter_pairs():
            for key, self_values in self_grouped.items():
                for self_value in self_values:
                    for other_value in other_grouped[key]:
                        yield key, (self_value, other_value)

        return FL(iter_pairs())

    def distinct(self) -> "FL":
        return self \
            .map(lambda e: (e, 1)) \
            .reduce_by_key(lambda x, y: 1) \
            .map(lambda e: e[0])

    def _group_by_key(self) -> defaultdict["FL"]:
        key_values = defaultdict(FL)
        for key, value, *_ in self:
            key_values[key].append(value)
        return key_values


def _get_indexes(pattern: pattern_type) -> Dict[str, List[int]]:
    def yield_in_addresses(pattern_, parent=None):
        if parent is None:
            parent = []
        for i, value in enumerate(pattern_):
            match value:
                case str() as name:
                    yield name, parent + [i]
                case tuple() as nested:
                    yield from yield_in_addresses(nested, parent + [i])
                case None:
                    pass
                case bad_type:
                    raise NotImplementedError(f"expected a tuple, str or None, got: {bad_type}")

    indexes = dict(yield_in_addresses(pattern))
    return indexes


def _get_named_values(indexes, obj) -> Dict[str, Any]:
    def get_value(address, obj_):
        for index in address:
            obj_ = obj_[index]
        return obj_

    named_values = {
        name: get_value(address, obj)
        for name, address
        in indexes.items()
    }
    return named_values
