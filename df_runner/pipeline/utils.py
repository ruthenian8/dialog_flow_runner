import collections
from typing import Union, List, Callable

from df_engine.core import Actor

from ..service.service import Service
from ..service.group import ServiceGroup


def pretty_format_component_info_dict(
    service: dict,
    show_wrappers: bool,
    offset: str = "",
    wrappers_key: str = "wrappers",
    type_key: str = "type",
    name_key: str = "name",
    indent: int = 4,
) -> str:
    indent = " " * indent
    representation = f"{offset}{service.get(type_key, '[None]')}%s:\n" % (
        f" '{service.get(name_key, '[None]')}'" if name_key in service else ""
    )
    for key, value in service.items():
        if key not in (type_key, name_key, wrappers_key) or (key == wrappers_key and show_wrappers):
            if isinstance(value, List):
                if len(value) > 0:
                    values = [
                        pretty_format_component_info_dict(instance, show_wrappers, f"{indent * 2}{offset}")
                        for instance in value
                    ]
                    value_str = "\n%s" % "\n".join(values)
                else:
                    value_str = "[None]"
            else:
                value_str = str(value)
            representation += f"{offset}{indent}{key}: {value_str}\n"
    return representation[:-1]


def rename_component_incrementing(
    service: Union[Service, ServiceGroup], collisions: List[Union[Service, ServiceGroup]]
):
    if isinstance(service, Service) and isinstance(service.handler, Actor):
        base_name = "actor"
    elif isinstance(service, Service) and isinstance(service.handler, Callable):
        base_name = service.handler.__name__
    elif isinstance(service, ServiceGroup):
        base_name = "service_group"
    else:
        base_name = "noname_service"
    name_index = 0
    while f"{base_name}_{name_index}" in [serv.name for serv in collisions]:
        name_index += 1
    return f"{base_name}_{name_index}"


def resolve_components_name_collisions(service_group: ServiceGroup):
    name_scope_level = 0
    checked_names = []
    while True:
        name_scope_level += 1
        flat_services = service_group.get_subgroups_and_services(recursion_level=name_scope_level)
        flat_services = [(path, service) for path, service in flat_services if service.name not in checked_names]
        if len(flat_services) == 0:
            break

        flat_services = [
            (f"{prefix}.{serv.name if serv.name is not None else ''}", serv) for prefix, serv in flat_services
        ]
        paths, services = list(zip(*flat_services))
        path_counter = collections.Counter(paths)

        for path, service in flat_services:
            if path_counter[path] > 1 or service.name is None:
                if path.endswith("."):
                    service.name = rename_component_incrementing(service, services)
                    service.path = f"{path}{service.name}"
                else:
                    raise Exception(f"User defined service name collision ({path})!!")
            else:
                service.path = path

        checked_names += [service.name for _, service in flat_services]
    return service_group
