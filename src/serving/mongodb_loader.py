"""MongoDB-specific loading and validation for Gold Analytics documents."""

from dataclasses import dataclass
from decimal import Decimal
from numbers import Integral, Real
from typing import Any, Iterable, Mapping


SERVING_SNAPSHOT = "historical_v1"
MISSING_PLAYTIME_ID = "MISSING"


@dataclass(frozen=True)
class IndexSpec:
    name: str
    fields: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class CollectionSpec:
    name: str
    key_field: str | None
    expected_count: int
    indexes: tuple[IndexSpec, ...] = ()


@dataclass(frozen=True)
class LoadStats:
    source_count: int
    matched_count: int
    modified_count: int
    upserted_count: int
    stale_deleted_count: int
    final_count: int


COLLECTION_SPECS = {
    "game_metrics": CollectionSpec(
        "game_metrics",
        "appid",
        50,
        (
            IndexSpec("recommendation_rate_desc", (("recommendation_rate", -1),)),
            IndexSpec("review_count_desc", (("review_count", -1),)),
        ),
    ),
    "genre_metrics": CollectionSpec(
        "genre_metrics",
        "genre",
        17,
        (
            IndexSpec("recommendation_rate_desc", (("recommendation_rate", -1),)),
            IndexSpec("review_count_desc", (("review_count", -1),)),
        ),
    ),
    "playtime_metrics": CollectionSpec(
        "playtime_metrics",
        "playtime_bucket",
        5,
    ),
    "free_paid_metrics": CollectionSpec(
        "free_paid_metrics",
        "game_type",
        2,
    ),
    "engagement_metrics": CollectionSpec(
        "engagement_metrics",
        "appid",
        50,
        (
            IndexSpec("recommendation_rate_desc", (("recommendation_rate", -1),)),
            IndexSpec(
                "avg_playtime_at_review_hours_desc",
                (("avg_playtime_at_review_hours", -1),),
            ),
        ),
    ),
    "label_profile": CollectionSpec("label_profile", None, 1),
    "platform_metrics": CollectionSpec(
        "platform_metrics",
        "platform",
        3,
    ),
    "category_metrics": CollectionSpec(
        "category_metrics",
        "category",
        59,
        (
            IndexSpec("review_count_desc", (("review_count", -1),)),
            IndexSpec("recommendation_rate_desc", (("recommendation_rate", -1),)),
        ),
    ),
    "purchase_metrics": CollectionSpec(
        "purchase_metrics",
        "purchase_source",
        2,
    ),
}


INTEGER_FIELDS = {
    "review_count",
    "positive_reviews",
    "negative_reviews",
    "game_count",
    "playtime_observed_count",
    "playtime_at_review_observed_count",
    "playtime_forever_observed_count",
    "total_rows",
    "unique_recommendationid",
    "distinct_appids",
    "positive_count",
    "negative_count",
    "free_review_count",
    "paid_review_count",
    "null_playtime_at_review",
    "null_playtime_forever",
    "null_price",
    "null_genres",
    "null_timestamp_created",
    "null_weighted_vote_score",
}

NUMERIC_FIELDS = INTEGER_FIELDS | {
    "recommendation_rate",
    "avg_playtime_hours",
    "min_price",
    "max_price",
    "avg_price",
    "avg_playtime_at_review_hours",
    "avg_playtime_forever_hours",
    "avg_votes_up",
    "avg_votes_funny",
    "avg_weighted_vote_score",
    "positive_percentage",
    "negative_percentage",
}

RATE_FIELDS = {
    "recommendation_rate",
    "positive_percentage",
    "negative_percentage",
}


def to_python_native(value: Any) -> Any:
    """Convert Spark/Python-like values into BSON-compatible native values."""
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    if isinstance(value, Mapping):
        return {str(key): to_python_native(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_python_native(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if value is not None and hasattr(value, "item"):
        try:
            return to_python_native(value.item())
        except (TypeError, ValueError):
            pass
    return value


def natural_id(collection_name: str, document: Mapping[str, Any]) -> Any:
    if collection_name not in COLLECTION_SPECS:
        raise KeyError(f"Unknown serving collection: {collection_name}")
    spec = COLLECTION_SPECS[collection_name]
    if collection_name == "label_profile":
        return "historical_baseline"

    value = document.get(spec.key_field)
    if collection_name == "playtime_metrics" and (
        value is None or (isinstance(value, str) and not value.strip())
    ):
        return MISSING_PLAYTIME_ID
    if value is None:
        raise ValueError(
            f"{collection_name} missing natural key field {spec.key_field}"
        )
    if isinstance(value, str) and not value:
        raise ValueError(
            f"{collection_name} has an empty natural key {spec.key_field}"
        )
    return value


def build_documents(
    collection_name: str,
    rows: Iterable[Any],
) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    identities: set[Any] = set()
    for row in rows:
        document = to_python_native(row)
        if not isinstance(document, dict):
            raise TypeError("Analytics rows must convert to dictionaries")
        identity = natural_id(collection_name, document)
        if identity in identities:
            raise ValueError(
                f"{collection_name} contains duplicate natural key {identity!r}"
            )
        identities.add(identity)
        document["_id"] = identity
        document["serving_snapshot"] = SERVING_SNAPSHOT
        documents.append(document)
    return documents


def upsert_snapshot(database, collection_name: str, documents: list[dict]) -> LoadStats:
    from pymongo import ReplaceOne

    if collection_name not in COLLECTION_SPECS:
        raise KeyError(f"Unknown serving collection: {collection_name}")
    if not documents:
        raise ValueError(f"Refusing to publish empty snapshot: {collection_name}")

    collection = database[collection_name]
    operations = [
        ReplaceOne({"_id": document["_id"]}, document, upsert=True)
        for document in documents
    ]
    result = collection.bulk_write(operations, ordered=False)
    identities = [document["_id"] for document in documents]
    stale_result = collection.delete_many({"_id": {"$nin": identities}})
    return LoadStats(
        source_count=len(documents),
        matched_count=result.matched_count,
        modified_count=result.modified_count,
        upserted_count=len(result.upserted_ids),
        stale_deleted_count=stale_result.deleted_count,
        final_count=collection.count_documents({}),
    )


def create_indexes(database) -> None:
    for spec in COLLECTION_SPECS.values():
        collection = database[spec.name]
        for index in spec.indexes:
            collection.create_index(list(index.fields), name=index.name)


def load_all_collections(
    database,
    documents: Mapping[str, list[dict]],
) -> dict[str, LoadStats]:
    if set(documents) != set(COLLECTION_SPECS):
        missing = sorted(set(COLLECTION_SPECS) - set(documents))
        extra = sorted(set(documents) - set(COLLECTION_SPECS))
        raise ValueError(f"Serving collection mismatch: missing={missing}, extra={extra}")
    stats = {
        name: upsert_snapshot(database, name, documents[name])
        for name in COLLECTION_SPECS
    }
    create_indexes(database)
    return stats


def _validate_numeric_document(collection_name: str, document: Mapping) -> None:
    for field in NUMERIC_FIELDS & set(document):
        value = document[field]
        if value is None:
            continue
        if field in INTEGER_FIELDS:
            valid = isinstance(value, Integral) and not isinstance(value, bool)
        else:
            valid = isinstance(value, Real) and not isinstance(value, bool)
        if not valid:
            raise RuntimeError(
                f"{collection_name} document {document['_id']!r} has "
                f"non-numeric {field}={value!r}"
            )
        if field in RATE_FIELDS and not 0 <= value <= 1:
            raise RuntimeError(
                f"{collection_name} document {document['_id']!r} has "
                f"invalid {field}={value!r}"
            )


def validate_database(database) -> tuple[dict[str, int], dict[str, int]]:
    counts: dict[str, int] = {}
    for name, spec in COLLECTION_SPECS.items():
        documents = list(database[name].find({}))
        counts[name] = len(documents)
        if counts[name] != spec.expected_count:
            raise RuntimeError(
                f"MongoDB {name} count {counts[name]} != {spec.expected_count}"
            )
        identities = [document["_id"] for document in documents]
        if len(set(identities)) != len(identities):
            raise RuntimeError(f"MongoDB {name} contains duplicate natural keys")
        for document in documents:
            if document["_id"] != natural_id(name, document):
                raise RuntimeError(f"MongoDB {name} has inconsistent natural key")
            _validate_numeric_document(name, document)

    profile = database["label_profile"].find_one(
        {"_id": "historical_baseline"}
    )
    expected_profile = {
        "total_rows": 25_000,
        "positive_count": 18_321,
        "negative_count": 6_679,
    }
    for field, expected in expected_profile.items():
        if profile.get(field) != expected:
            raise RuntimeError(
                f"MongoDB label_profile {field}={profile.get(field)} != {expected}"
            )

    totals = {
        "reviews": profile["total_rows"],
        "positive": profile["positive_count"],
        "negative": profile["negative_count"],
        "game_reviews": sum(
            document["review_count"]
            for document in database["game_metrics"].find({}, {"review_count": 1})
        ),
        "free_paid_reviews": sum(
            document["review_count"]
            for document in database["free_paid_metrics"].find(
                {}, {"review_count": 1}
            )
        ),
        "purchase_reviews": sum(
            document["review_count"]
            for document in database["purchase_metrics"].find(
                {}, {"review_count": 1}
            )
        ),
    }
    for field in ("game_reviews", "free_paid_reviews", "purchase_reviews"):
        if totals[field] != 25_000:
            raise RuntimeError(f"MongoDB {field}={totals[field]} != 25000")
    return counts, totals


def sample_queries(database) -> dict[str, Any]:
    return {
        "top_10_games": list(
            database["game_metrics"]
            .find(
                {},
                {
                    "game_name": 1,
                    "review_count": 1,
                    "recommendation_rate": 1,
                },
            )
            .sort([("recommendation_rate", -1), ("_id", 1)])
            .limit(10)
        ),
        "genres_by_review_count": list(
            database["genre_metrics"]
            .find({}, {"genre": 1, "review_count": 1, "recommendation_rate": 1})
            .sort([("review_count", -1), ("_id", 1)])
        ),
        "playtime_buckets": list(
            database["playtime_metrics"].find({}).sort("_id", 1)
        ),
        "free_vs_paid": list(
            database["free_paid_metrics"].find({}).sort("_id", 1)
        ),
        "historical_label_profile": database["label_profile"].find_one(
            {"_id": "historical_baseline"}
        ),
        "purchase_sources": list(
            database["purchase_metrics"].find({}).sort("_id", 1)
        ),
    }


def index_inventory(database) -> dict[str, dict]:
    return {
        name: database[name].index_information()
        for name in COLLECTION_SPECS
    }
