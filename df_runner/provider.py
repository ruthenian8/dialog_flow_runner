import uuid
from abc import abstractmethod, ABC
from typing import Optional


class AbsProvider(ABC):
    def __init__(self):
        self.ctx_id = uuid.uuid4()

    @abstractmethod
    def init(self):
        raise NotImplementedError

    @abstractmethod
    def request(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def response(self, response: str):
        raise NotImplementedError


class CLIProvider(AbsProvider):
    def __init__(
        self,
        intro: Optional[str] = None,
        prompt_request: str = "request: ",
        prompt_response: str = "response: ",
    ):
        super().__init__()
        self.intro: Optional[str] = intro
        self.prompt_request: str = prompt_request
        self.prompt_response: str = prompt_response

    def init(self):
        if self.intro is not None:
            print(self.intro)

    def request(self) -> str:
        return input(self.prompt_request)

    def response(self, response: str):
        print(f"{self.prompt_response}{response}")