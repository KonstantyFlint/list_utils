from collections import defaultdict
from typing import Callable, Iterable, Any, Tuple, Union, Dict, List

pattern_type = Union[None, str, Tuple["Pattern", ...]]


class FunctionalList(list):

    def map(self, mapper: Callable[[Any], Any]) -> "FunctionalList":
        return FunctionalList(mapper(value) for value in self)

    def flatten(self) -> "FunctionalList":
        def iter_flatten():
            for iterable in self:
                for value in iterable:
                    yield value

        return FunctionalList(iter_flatten())

    def flat_map(self, mapper: Callable[[Any], Iterable[Any]]) -> "FunctionalList":
        return self.map(mapper).flatten()

    def reduce(self, reducer: Callable[[Any, Any], Any]) -> Any:
        if len(self) == 0:
            return None
        iterator = iter(self)
        value = next(iterator)
        for next_value in iterator:
            value = reducer(value, next_value)
        return value

    def reduce_by_key(self, reducer: Callable[[Any, Any], Any]) -> "FunctionalList":
        key_values = FunctionalList(self._group_by_key().items())
        return key_values.map(lambda e: (e[0], e[1].reduce(reducer)))

    def join_by_key(self, other: "FunctionalList") -> "FunctionalList":
        self_grouped = self._group_by_key()
        other_grouped = other._group_by_key()

        def iter_pairs():
            for key, self_values in self_grouped.items():
                for self_value in self_values:
                    for other_value in other_grouped[key]:
                        yield key, (self_value, other_value)

        return FunctionalList(iter_pairs())

    def join_by_custom_key(self, other: "Fl", key_getter: Callable, other_key_getter: Callable, keep_key=False):
        self_with_key = self.map(lambda obj: (key_getter(obj), obj))
        other_with_key = other.map(lambda obj: (other_key_getter(obj), obj))
        joined = self_with_key.join_by_key(other_with_key)
        if not keep_key:
            joined = joined.map(lambda obj: obj[1])
        return joined

    def distinct(self) -> "FunctionalList":
        return self \
            .map(lambda e: (e, 1)) \
            .reduce_by_key(lambda x, y: 1) \
            .map(lambda e: e[0])

    def _group_by_key(self) -> defaultdict["FunctionalList"]:
        key_values = defaultdict(FunctionalList)
        for key, value, *_ in self:
            key_values[key].append(value)
        return key_values
