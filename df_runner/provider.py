import logging
import uuid
from abc import abstractmethod, ABC
from asyncio import sleep, run
from typing import Optional, Any, List, Tuple

from df_engine.core import Context

from .types import ProviderFunction, LoopFunction

logger = logging.getLogger(__name__)


class AbsProvider(ABC):
    """
    Class that represents a provider used for communication between runner and user.
    """

    def __init__(self):
        self._callback: Optional[ProviderFunction] = None

    async def run(self, callback: ProviderFunction, *args, **kwargs):
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

    def __init__(self, timeout: int = 0):
        super().__init__()
        self._timeout = timeout

    @abstractmethod
    def _request(self) -> Tuple[List[Any], List[Any]]:
        """
        Method used for sending user a request for input.
        """
        raise NotImplementedError

    @abstractmethod
    def _respond(self, response: List[Context]):
        """
        Method used for sending user a response for his last input.
        """
        raise NotImplementedError

    def _except(self, e: Exception):
        """
        Method that is called on polling cycle exceptions.
        """
        logger.error(e)

    async def run(self, callback: ProviderFunction, loop: LoopFunction = lambda: True, *args, **kwargs):
        """
        Method, running a request - response cycle in a loop, sleeping for self._timeout seconds after each iteration.
        """
        await super().run(callback)
        while loop():
            try:
                requests, ctx_ids = self._request()
                responses = [await self._callback(request, ctx_id) for request, ctx_id in zip(requests, ctx_ids)]
                self._respond(responses)
                await sleep(self._timeout)

            except Exception as e:
                self._except(e)
                break


class CallbackProvider(AbsProvider):
    """
    Callback provider is waiting for user input and answers once it gets one.
    """

    async def on_request_async(self, request: Any, ctx_id: Any) -> Any:
        """
        Method invoked on user input, should run await self._callback function (if any).
        Use this in async context, await will not work in sync.
        """
        response = await self._callback(request, ctx_id)
        return response.last_response

    def on_request_sync(self, request: Any, ctx_id: Any) -> Any:
        """
        Method invoked on user input, should run self._callback function (if any).
        Use this in sync context, asyncio.run() will produce error in async.
        """
        response = run(self._callback(request, ctx_id))
        return response.last_response


class CLIProvider(PollingProvider):
    """
    Command line provider - the default provider for each runner.
    """

    def __init__(
        self, intro: Optional[str] = None, prompt_request: str = "request: ", prompt_response: str = "response: "
    ):
        super().__init__()
        self.ctx_id: Optional[Any] = None
        self._intro: Optional[str] = intro
        self._prompt_request: str = prompt_request
        self._prompt_response: str = prompt_response

    async def run(self, callback: ProviderFunction, *args, **kwargs):
        self.ctx_id = uuid.uuid4()
        if self._intro is not None:
            print(self._intro)
        await super().run(callback)

    def _request(self) -> Tuple[List[Any], List[Any]]:
        return [input(self._prompt_request)], [self.ctx_id]

    def _respond(self, response: List[Context]):
        print(f"{self._prompt_response}{response[0].last_response}")
