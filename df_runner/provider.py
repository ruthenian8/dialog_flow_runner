import uuid
from abc import abstractmethod, ABC
from typing import Optional

from df_engine.core import Context

from df_runner import Runner


class AbsProvider(ABC):
    @abstractmethod
    def run(self, runner: Runner):
        raise NotImplementedError


class CLIProvider(AbsProvider):
    def __init__(
        self,
        intro: Optional[str] = None,
        prompt_request: str = "request: ",
        prompt_response: str = "response: ",
    ):
        self.intro: Optional[str] = intro
        self.prompt_request: str = prompt_request
        self.prompt_response: str = prompt_response

    def run(self, runner: Runner):
        ctx_id = uuid.uuid4()
        if self.intro is not None:
            print(self.intro)
        while True:
            request = input(self.prompt_request)
            ctx: Context = runner.request_handler(ctx_id, request)
            print(f"{self.prompt_response}{ctx.last_response}")
