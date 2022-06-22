from typing import Callable

from flask import Flask, request as flask_request

from df_runner import CallbackProvider, ProviderFunction


class FlaskProvider(CallbackProvider):
    """

    """

    def __init__(
        self,
        flask_app: Flask,
        route_name: str = '/df_provider',
        route_options: dict = None,
        param_name: str = 'data',
        process: Callable[[str], str] = lambda req: req
    ):
        super().__init__()
        self._app = flask_app
        self._route_name = route_name
        self._route_options = dict() if route_options is None else route_options
        self._param_name = param_name
        self._process = process

    def run(self, callback: ProviderFunction):
        def route():
            req = flask_request.args.get(self._param_name)
            return self._on_request(self._process(req))
        super().run(callback)
        self._app.add_url_rule(self._route_name, view_func=route, **self._route_options)
