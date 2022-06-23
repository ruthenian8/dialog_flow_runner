import logging
from typing import Optional, Union, Dict, Callable

from df_engine.core import Actor, Context
from pydantic import BaseModel, Extra
from typing_extensions import Self

from df_runner import ServiceFunction, ServiceCondition
from df_runner.conditions import always_start_condition


logger = logging.getLogger(__name__)


class Service(BaseModel):
    """
    Extension class for annotation functions, may be created from dict.

    It accepts:
        service - an annotation function or an actor
        name (optionally) - custom service name (used for identification)
            NB! if name is not provided, it will be generated from Actor, Function or dict.
        timeout (optionally) - the time period the service is allowed to run, it will be killed on exception, default: 1000 ms
        start_condition (optionally) - requirement for service to start, service will run only if this returned True, default: always_start_condition
    """

    service: Union[Actor, ServiceFunction]
    name: Optional[str] = None
    timeout: int = -1
    start_condition: ServiceCondition = always_start_condition

    class Config:
        extra = Extra.allow

    def __call__(self, context: Context, actor: Optional[Actor] = None, *args, **kwargs) -> Context:
        """
        Service may be executed, as actor in case it's an actor or as function in case it's an annotator function.
        It also sets named variables in context.framework_states for other services start_conditions.
        If execution fails the error is caught here.
        TODO: add asyncio with timeouts.
        """
        try:
            if not isinstance(self.service, Actor):
                if actor is None:
                    raise AttributeError(f"For service {self.name} no actor has been provided!")
                else:
                    actor = self.service

            if self.start_condition(context, self.service):
                if isinstance(self.service, Actor):
                    context = self.service(context)
                else:
                    context = self.service(context, actor)
            else:
                raise BrokenPipeError(f"For service {self.name} start_condition hasn't been met!")

            context.framework_states['RUNNER'][f'{self.name}-success'] = True

        except Exception as e:
            context.framework_states['RUNNER'][f'{self.name}-success'] = False
            if isinstance(e, BrokenPipeError):
                logger.info(e)
            else:
                logger.error(e)

        context.framework_states['RUNNER'][f'{self.name}-finished'] = True
        return context

    @staticmethod
    def _get_name(
        service: Union[Actor, Dict, ServiceFunction],
        naming: Optional[Dict[str, int]] = None,
        name: Optional[str] = None
    ) -> str:
        """
        Method for name generation.
        Name is generated using following convention:
            actor: 'actor-[NUMBER]'
            function: 'func-[REPR]-[NUMBER]'
            service object: 'obj-[NUMBER]'
        If user provided name uses same syntax it will be changed to auto-generated.
        """
        if name is not None and not (name.startswith('actor-') or name.startswith('func-') or name.startswith('obj-')):
            return name
        elif name is not None:
            logger.warning(f"User defined name for service '{name}' violates naming convention, the service will be renamed")

        if isinstance(service, Actor):
            name = 'actor'
        elif isinstance(service, Service):
            name = 'obj'
        elif isinstance(service, Callable):
            name = f'func-{service.__name__}'
        else:
            name = 'unknown'

        if naming is not None:
            number = naming.get(name, 0)
            naming[name] = number + 1
            return f'{name}-{number}'
        else:
            return name

    @classmethod
    def cast(
        cls,
        service: Union[Actor, Dict, ServiceFunction, Self],
        naming: Optional[Dict[str, int]] = None,
        name: Optional[str] = None,
        timeout: int = -1,
        start_condition: ServiceCondition = always_start_condition
    ):
        """
        Method for service creation from actor, function or dict.
        No other sources are accepted (yet).
        """
        if isinstance(service, Service):
            service.name = cls._get_name(service, naming, service.name)
            return service
        elif isinstance(service, Actor) or isinstance(service, Callable):
            return cls(
                service=service,
                name=cls._get_name(service, naming, name),
                timeout=timeout,
                start_condition=start_condition
            )
        else:
            raise Exception(f"Unknown type of service {service}")
