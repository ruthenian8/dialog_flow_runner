from asyncio import get_running_loop, run
from collections import Counter
from typing import Awaitable, Any, Coroutine, Union, List

from .service import Service
from .service_group import ServiceGroup


def run_in_current_or_new_loop(future: Union[Awaitable, Coroutine]) -> Any:
    try:
        loop = get_running_loop()
        if loop.is_running():
            return loop.create_task(future)
        else:
            return loop.run_until_complete(future)
    except RuntimeError:
        return run(future)


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
