from __future__ import annotations

from pymongo import MongoClient
from pymongo.database import Database

from .config import COLLECTIONS, load_settings


_client: MongoClient | None = None
_database: Database | None = None


def get_database() -> Database:
    global _client, _database

    if _database is not None:
        return _database

    settings = load_settings()
    _client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=10000)
    _client.admin.command("ping")
    _database = _client[settings.mongo_db_name]
    return _database


def ensure_indexes(database: Database) -> None:
    for collection_name in COLLECTIONS:
        database[collection_name].create_index("id", unique=True, sparse=True)

    database["project_assignments"].create_index(
        [("project_id", 1), ("team_member_id", 1)],
        unique=True,
        sparse=True,
    )


def close_database() -> None:
    global _client, _database
    if _client is not None:
        _client.close()
    _client = None
    _database = None