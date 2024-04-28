from typing import Tuple, Callable, Any

from list_utils.functional_list import FunctionalList

FromStr = Callable[[str], Any]


def read_file(filename: str, factories: Tuple[FromStr, ...], separator: str = ",") -> FunctionalList:
    return FunctionalList(iter_file(filename, factories, separator))


def iter_file(filename: str, factories: Tuple[FromStr, ...], separator: str = ","):
    with open(filename) as file:
        for row in file.readlines():
            yield read_row(row, factories, separator)


def read_row(row: str, factories: Tuple[FromStr, ...], separator: str) -> Tuple[Any, ...]:
    parts_str = row.strip().split(sep=separator)
    assert len(parts_str) == len(factories)
    parts = []
    for (part, factory) in zip(parts_str, factories):
        parts.append(factory(part.strip()))
    return tuple(parts)
