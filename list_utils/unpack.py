from typing import Tuple, Callable, Dict, List, Any


def _get_name_to_address(structure: Tuple) -> Dict[str, List[int]]:
    def iter_named_addresses(structure_, parent=None):
        if parent is None:
            parent = []
        for i, value in enumerate(structure_):
            match value:
                case str() as name:
                    yield name, parent + [i]
                case tuple() as nested:
                    yield from iter_named_addresses(nested, parent + [i])
                case None:
                    pass
                case bad_type:
                    raise NotImplementedError(f"expected a tuple, str or None, got: {bad_type}")

    indexes = dict(iter_named_addresses(structure))
    return indexes


def _get_name_to_value(name_to_address, obj) -> Dict[str, Any]:
    def get_value(address, obj_):
        for index in address:
            obj_ = obj_[index]
        return obj_

    named_values = {
        name: get_value(address, obj)
        for name, address
        in name_to_address.items()
    }
    return named_values


def unpack(structure: Tuple, fun: Callable):
    name_to_address = _get_name_to_address(structure)

    def wrapper(obj):
        name_to_value = _get_name_to_value(name_to_address, obj)
        return fun(**name_to_value)

    return wrapper
