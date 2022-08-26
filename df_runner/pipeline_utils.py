from collections import Counter
from typing import Union, List

from .service import Service
from .service_group import ServiceGroup


def print_instance_dict(
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
                    values = [print_instance_dict(instance, show_wrappers, f"\t\t{offset}") for instance in value]
                    value_str = "\n%s" % "\n".join(values)
                else:
                    value_str = "[None]"
            else:
                value_str = str(value)
            representation += f"{offset}\t{key}: {value_str}\n"
    return representation[:-1]


def create_service_name(base_name, services: List[Union[Service, ServiceGroup]]):
    name_index = 0
    while f"{base_name}_{name_index}" in [serv.name for serv in services]:
        name_index += 1
    return f"{base_name}_{name_index}"


def rename_same_service_prefix(services_pipeline: ServiceGroup):
    name_scoup_level = 0
    checked_names = 0
    while True:
        name_scoup_level += 1
        flat_services = services_pipeline.get_subgroups_and_services(recursion_level=name_scoup_level)
        flat_services = flat_services[checked_names:]
        if not flat_services:
            break
        checked_names += len(flat_services)
        flat_services = [(f"{prefix}.{serv.name}", serv) for prefix, serv in flat_services]
        paths, services = list(zip(*flat_services))
        path_counter = Counter(paths)

        if max(path_counter.values()) > 1:
            # rename procedure for same paths
            for path, service in flat_services:
                if path_counter[path] > 1:
                    service.name = create_service_name(service.name, services)
    return services_pipeline
