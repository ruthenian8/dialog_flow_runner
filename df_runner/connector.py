import logging
from typing import Any

from df_db_connector import DBConnector


logger = logging.getLogger(__name__)


class CLIConnector(DBConnector):
    def __init__(self):
        super().__init__("")
        logging.basicConfig(level=logging.INFO)
        self._container = dict()

    def _log(self, key: str):
        logging.info(f"Database entry acquired: {key} -> {self._container[key]}")

    def __getitem__(self, key: str) -> Any:
        self._log(key)
        return self._container[key]

    def __setitem__(self, key: str, value: dict):
        self._container[key] = value
        self._log(key)

    def __delitem__(self, key: str):
        self._log(key)
        del self._container[key]

    def __contains__(self, key: str) -> bool:
        logging.info(f"Database contains key: {key}")
        return key in self._container

    def __len__(self) -> int:
        logging.info(f"Database length: {len(self._container)}")
        return len(self._container)

    def clear(self) -> None:
        logging.info("Database cleared")
        return self._container.clear()
