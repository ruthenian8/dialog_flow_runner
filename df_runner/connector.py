from df_db_connector import DBConnector, threadsafe_method
from df_engine.core import Context


class DefaultConnector(DBConnector):
    def __init__(self):
        super().__init__("")

    def __contains__(self, key: str) -> bool:
        return False

    def __len__(self) -> int:
        return -1

    def get(self, key: str, default=None):
        return default

    @threadsafe_method
    def __getitem__(self, key: str) -> Context:
        return Context.cast({})

    @threadsafe_method
    def __setitem__(self, key: str, value: Context) -> None:
        print(f"Database entry added: {key} -> {value}")

    @threadsafe_method
    def __delitem__(self, key: str):
        print(f"Database entry deleted: {key}")

    @threadsafe_method
    def clear(self):
        print("Database entries cleared")
