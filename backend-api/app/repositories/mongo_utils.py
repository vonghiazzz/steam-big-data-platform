from collections.abc import Mapping
from datetime import datetime
from typing import Any

from bson import ObjectId


def _serialize_value(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, datetime):
        return value
    return value


def serialize_document(document: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): _serialize_value(value)
        for key, value in document.items()
        if key != "_id"
    }