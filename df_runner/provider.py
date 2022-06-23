import asyncio
import uuid
from abc import abstractmethod, ABC
from typing import Optional, Any

from df_runner import ProviderFunction


class AbsProvider(ABC):
    """
    Class that represents a provider used for communication between runner and user.
    """

    def __init__(self):
        self.ctx_id: Optional[Any] = None
        self._callback: Optional[ProviderFunction] = None

    def run(self, callback: ProviderFunction):
        """
        Method invoked when user first interacts with the runner and dialog starts.
        A good place to generate self.ctx_id - a unique ID of the dialog.
        May be used for sending self._intro - an introduction message.
        :callback: - a function that is run every time user provider an input, returns runner answer.
        """
        self._callback = callback


class PollingProvider(AbsProvider):
    """
    Polling provider runs in a loop, constantly asking user for a new input.
    """

    def __init__(
        self,
        timeout: int = 0
    ):
        super().__init__()
        self._timeout = timeout

    @abstractmethod
    def _request(self) -> str:
        """
        Method used for sending user a request for input.
        """
        raise NotImplementedError

    @abstractmethod
    def _respond(self, response: str):
        """
        Method used for sending user a response for his last input.
        """
        raise NotImplementedError

    def run(self, callback: ProviderFunction):
        """
        Method, running a request - response cycle in a loop, sleeping for self._timeout seconds after each iteration.
        """
        super().run(callback)
        while True:
            request = self._request()
            self._respond(self._callback(request).last_response)
            asyncio.sleep(self._timeout)


class CallbackProvider(AbsProvider):
    """
    Callback provider is waiting for user input and answers once it gets one.
    """

    def _on_request(self, request: Any) -> Any:
        """
        Method invoked on user input, should run self._callback function (if any).
        """
        return self._callback(request).last_response


class CLIProvider(PollingProvider):
    """
    Command line provider - the default provider for each runner.
    """

    def __init__(
        self,
        intro: Optional[str] = None,
        prompt_request: str = "request: ",
        prompt_response: str = "response: "
    ):
        super().__init__()
        self._intro: Optional[str] = intro
        self._prompt_request: str = prompt_request
        self._prompt_response: str = prompt_response

    def run(self, callback: ProviderFunction):
        self.ctx_id = uuid.uuid4()
        if self._intro is not None:
            print(self._intro)
        super().run(callback)

    def _request(self) -> str:
        return input(self._prompt_request)

    def _respond(self, response: str):
        print(f"{self._prompt_response}{response}")
