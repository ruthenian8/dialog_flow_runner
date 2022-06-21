from typing import Callable, Tuple

from df_engine.core import Context


AnnotatorFunctionType = Callable[[Context], Context]

ServiceFunctionType = Callable[[Context], Tuple[Context, bool]]

ServiceConditionType = Callable[[Context], bool]
