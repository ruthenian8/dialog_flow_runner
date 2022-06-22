from typing import Callable

from df_engine.core import Context, Actor


ServiceFunction = Callable[[Context, Actor], Context]

ServiceCondition = Callable[[Context, Actor], bool]

WrapperFunction = Callable[[Context, Actor], None]
