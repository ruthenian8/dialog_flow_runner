from ctypes import Union
from typing import Callable, Dict, List, Optional
from df_db_connector import DBAbstractConnector
from df_engine.core import Actor


# pipeline = {
#     "requester": telegram,
#     "db": db_connector,
#     "services": [
#           db_connector, 
#           telegram,
            # prepoc,
#         {
#             "service": prepoc,
#             "timeout": 1000,
#             "start_condition": condition.wait_ok(prepoc0),
#         },
#         actor,
#         cust_func,
#         {
#             "service": postproc,
#             # "name": "postproc",
#             "timeout": 1000,
#             "start_condition": condition,
#         },
#     ],
# }

