from collections import Counter
from typing import Union, List

from ..service.service import Service
from ..service.group import ServiceGroup


def print_component_info_dict(
    service: dict,
    show_wrappers: bool,
    offset: str = "",
    wrappers_key: str = "wrappers",
    type_key: str = "type",
    name_key: str = "name",
) -> str:
    representation = f"{offset}{service.get(type_key, '[None]')}%s:\n" % (
        f" '{service.get(name_key, '[None]')}'" if name_key in service else ""
    )
    for key, value in service.items():
        if key not in (type_key, name_key, wrappers_key) or (key == wrappers_key and show_wrappers):
            if isinstance(value, List):
                if len(value) > 0:
                    values = [print_component_info_dict(instance, show_wrappers, f"\t\t{offset}") for instance in value]
                    value_str = "\n%s" % "\n".join(values)
                else:
                    value_str = "[None]"
            else:
                value_str = str(value)
            representation += f"{offset}\t{key}: {value_str}\n"
    return representation[:-1]


def rename_component_incrementing(base_name: str, services: List[Union[Service, ServiceGroup]]):
    name_index = 0
    while f"{base_name}_{name_index}" in [serv.name for serv in services]:
        name_index += 1
    return f"{base_name}_{name_index}"


def resolve_components_name_collisions(service_group: ServiceGroup):  # FIXME: WARNING! services with same name but different paths can co-exist! Why not recursion? Complex code.
    name_scope_level = 0
    checked_names = 0
    while True:
        name_scope_level += 1
        flat_services = service_group.get_subgroups_and_services(recursion_level=name_scope_level)
        flat_services = flat_services[checked_names:]
        if not flat_services:  # FIXME: obscure syntax
            break

        checked_names += len(flat_services)
        flat_services = [(f"{prefix}.{serv.name}", serv) for prefix, serv in flat_services]
        paths, services = list(zip(*flat_services))
        path_counter = Counter(paths)

        for path, service in flat_services:
            if path_counter[path] > 1:
                service.name = rename_component_incrementing(service.name, services)
    return service_group
