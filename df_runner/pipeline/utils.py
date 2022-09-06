import collections
from typing import Union, List, Callable, Optional

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
    """
    Function for dumping any pipeline components info dictionary (received from `info_dict` property) as a string.
    Resulting string is formatted with YAML-like format, however it's not strict and shouldn't be parsed.
    However, most preferable usage is via `pipeline.pretty_format`.
    :service: (required) - pipeline components info dictionary.
    :show_wrappers: (required) - whether to include Wrappers or not (could be many and/or generated).
    :offset: - current level new line offset.
    :wrappers_key: - key that is mapped to Wrappers lists.
    :type_key: - key that is mapped to components type name.
    :name_key: - key that is mapped to components name.
    :indent: - current level new line offset (whitespace number).
    Returns formatted string.
    """
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
    service: Union[Service, ServiceGroup], collisions: Optional[List[Union[Service, ServiceGroup]]] = None
):
    """
    Function for generating new name for a pipeline component,
        that has similar name with other components in the same group.
    The name is generated according to these rules:
        1. If service's handler is Actor, it is named 'actor'
        2. If service's handler is Callable, it is named after this callable
        3. If it's a service group, it is named 'service_group'
        4. Otherwise, it is names 'noname_service'
        5. After that, '_[NUMBER]' is added to the resulting name,
            where number is number of components with the same name in current service group.
            NB! This happens only if `collisions` argument is not None.
    :service: (required) - service to be renamed.
    :collisions: - services in the same service group as service.
    Returns string - generated name.
    """
    if isinstance(service, Service) and isinstance(service.handler, Actor):
        base_name = "actor"
    elif isinstance(service, Service) and isinstance(service.handler, Callable):
        base_name = service.handler.__name__
    elif isinstance(service, ServiceGroup):
        base_name = "service_group"
    else:
        base_name = "noname_service"
    if collisions is None:
        return base_name
    else:
        name_index = 0
        while f"{base_name}_{name_index}" in [serv.name for serv in collisions]:
            name_index += 1
        return f"{base_name}_{name_index}"


def finalize_service_group(service_group: ServiceGroup, path: str = ".") -> Actor:
    """
    Function that iterates through a service group (and all its subgroups),
        finalizing component's names and paths in it.
    Components are renamed only if user didn't set a name for them. Their paths are also generated here.
    It also searches for Actor in th group, throwing exception if  no actor or multiple actors found.
    :service_group: - service group to resolve name collisions in.
    Returns Actor.
    """
    actor = None
    names_counter = collections.Counter([component.name for component in service_group.services])
    for component in service_group.services:
        if component.name is None:
            if names_counter[component.name] > 1:
                component.name = rename_component_incrementing(component, service_group.services)
            elif names_counter[component.name] == 1:
                component.name = rename_component_incrementing(component, None)
        elif names_counter[component.name] > 1:
            raise Exception(f"User defined service name collision ({path})!")
        component.path = f"{path}.{component.name}"

        if isinstance(component, Service) and isinstance(component.handler, Actor):
            current_actor = component.handler
        elif isinstance(component, ServiceGroup):
            current_actor = finalize_service_group(component, path)
        else:
            current_actor = None

        if current_actor is not None:
            if actor is None:
                actor = current_actor
            else:
                raise Exception(f"More than one actor found in group ({path})!")
    return actor
