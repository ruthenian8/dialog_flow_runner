from typing import List, Tuple
from .service_group import ServiceGroup
from .service import Service


def get_flat_services_list(group: ServiceGroup, prefix: str = "") -> List[Tuple[str, Service]]:
    """
    Returns a copy of created inner services flat array used during actual pipeline running.
    Might be used for debugging purposes.
    """
    services = list()
    prefix = f"{prefix}.{group.name}"
    for service in group.annotators:
        if isinstance(service, Service):
            services += [(prefix, service)]
        else:
            services += get_flat_services_list(service, prefix)
    return services
