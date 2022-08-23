import logging
import uuid
from abc import abstractmethod, ABC
from asyncio import sleep
from typing import Optional, Any, List, Tuple, Union, Awaitable

from df_engine.core import Context

from .types import PipelineRunnerFunction, PollingProviderLoopFunction
from .utils import run_in_current_or_new_loop

logger = logging.getLogger(__name__)


class AbsProvider(ABC):
    """
    Class that represents a provider used for communication between pipeline and users.
    It is responsible for connection between user and provider, as well as for request-response transactions.
    """

    def __init__(self):
        self._pipeline_runner: Optional[PipelineRunnerFunction] = None

    async def run(self, pipeline_runner: PipelineRunnerFunction):
        """
        Method invoked when provider is instantiated and connection is established.
        May be used for sending an introduction message or displaying general bot information.
        :pipeline_runner: - a function that should return pipeline response to user request; usually it's a `Pipeline._run_pipeline(request, ctx_id)` function.
        """
        self._pipeline_runner = pipeline_runner


class PollingProvider(AbsProvider):
    """
    Polling provider runs in a loop, constantly asking users for a new input.
    """

    @abstractmethod
    def _request(self) -> List[Tuple[Any, Any]]:
        """
        Method used for sending users request for their input.
        Returns a list of tuples: user inputs and context ids (any user ids) associated with inputs.
        """
        raise NotImplementedError

    @abstractmethod
    def _respond(self, response: List[Context]):
        """
        Method used for sending users responses for their last input.
        :response: - a list of contexts, representing dialogs with the users; `last_response`, `id` and some dialog info can be extracted from there.
        """
        raise NotImplementedError

    def _on_exception(self, e: BaseException):
        """
        Method that is called on polling cycle exceptions, in some cases it should show users the exception.
        By default, it logs all exit exceptions to `info` log and all non-exit exceptions to `error`.
        :e: - the exception.
        """
        if isinstance(e, Exception):
            logger.error("Exception in %s loop!\n%s", type(self).__name__, str(e))
        else:
            logger.info("%s has stopped polling.", type(self).__name__)

    async def run(self, pipeline_runner: PipelineRunnerFunction, loop: PollingProviderLoopFunction = lambda: True, timeout: int = 0):
        """
        Method, running a request - response cycle in a loop.
        The looping behaviour is determined by :loop: and :timeout:, for most cases the loop itself shouldn't be overridden.
        :loop: - a function that determines whether polling should be continued; called in each cycle, should return True to continue polling or False to stop.
        :timeout: - a time interval between polls (in seconds).
        """
        await super().run(pipeline_runner)
        while loop():
            try:
                user_updates = self._request()
                responses = [await self._pipeline_runner(request, ctx_id) for request, ctx_id in user_updates]
                self._respond(responses)
                await sleep(timeout)

            except BaseException as e:
                self._on_exception(e)
                break


class CallbackProvider(AbsProvider):
    """
    Callback provider is waiting for user input and answers once it gets one.
    """

    def on_request(self, request: Any, ctx_id: Any) -> Union[Context, Awaitable[Context]]:
        """
        Method invoked on user input.
        This method works just like `Pipeline.__call__(request, ctx_id)`, however callback provider may contain additional functionality (e.g. for external API accessing).
        :request: - user input.
        :ctx_id: - any unique id that will be associated with dialog between this user and pipeline.
        Returns a context OR an awaitable, returning context; the context represents dialog with the user; `last_response`, `id` and some dialog info can be extracted from there.
        WARNING! This method can be run both in sync and async contexts, however in async context it SHOULD be awaited.
        """
        return run_in_current_or_new_loop(self._pipeline_runner(request, ctx_id))


class CLIProvider(PollingProvider):
    """
    Command line provider - the default provider, communicating with user via STDIN/STDOUT.
    This provider can maintain dialog with one user at a time only.
    """

    def __init__(
        self, intro: Optional[str] = None, prompt_request: str = "request: ", prompt_response: str = "response: "
    ):
        super().__init__()
        self._ctx_id: Optional[Any] = None
        self._intro: Optional[str] = intro
        self._prompt_request: str = prompt_request
        self._prompt_response: str = prompt_response

    def _request(self) -> List[Tuple[Any, Any]]:
        return [(input(self._prompt_request), self._ctx_id)]

    def _respond(self, response: List[Context]):
        print(f"{self._prompt_response}{response[0].last_response}")

    async def run(self, pipeline_runner: PipelineRunnerFunction, **kwargs):
        """
        The CLIProvider generates new dialog id used to user identification on each `run` call.
        :kwargs: - argument, added for compatibility with super class, it shouldn't be used normally.
        """
        self._ctx_id = uuid.uuid4()
        if self._intro is not None:
            print(self._intro)
        await super().run(pipeline_runner, **kwargs)
