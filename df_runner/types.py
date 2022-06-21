from typing import Callable, Tuple

from df_engine.core import Context, Actor


AnnotatorFunctionType = Callable[[Context, Actor], Context]

ServiceFunctionType = Callable[[Context, Actor], Tuple[Context, bool]]

ServiceConditionType = Callable[[Context, Actor], bool]
