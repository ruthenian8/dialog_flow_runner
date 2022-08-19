import logging
from typing import Optional, Dict, Any, Set, Callable


logger = logging.getLogger(__name__)


class Named:
    name: Optional[str] = None

    @staticmethod
    def _get_name(
        this: Any,
        name_rule: Callable[[Any], str],
        forbidden_names: Set[str],
        naming: Optional[Dict[str, int]] = None,
        given_name: Optional[str] = None,
    ) -> str:
        """
        Method for name generation.
        Name is generated using following convention:
            actor: 'actor_[NUMBER]'
            function: 'func_[REPR]_[NUMBER]'
            service object: 'obj_[NUMBER]'
        If user provided name uses same syntax it will be changed to auto-generated.
        """
        if given_name is not None:
            forbidden = [given_name.startswith(name) for name in forbidden_names]
            if not any(forbidden):
                if naming is not None:
                    if given_name in naming:
                        raise Exception(f"User defined service name collision: {given_name}")
                    else:
                        naming[given_name] = True
                return given_name
            else:
                type_name = type(this).__name__
                logger.warning(
                    f"User defined name for {type_name} '{given_name}' "
                    f"violates naming convention, the {type_name} will be renamed"
                )

        name = name_rule(this)

        if naming is not None:
            number = naming.get(name, 0)
            naming[name] = number + 1
            return f"{name}_{number}"
        else:
            return name
