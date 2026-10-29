"""Helpers that turn database values into JSON-safe structures."""
from __future__ import annotations

from datetime import date, datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def jsonable(value):
    """Convert Mongo/Neo4j/Redis values (datetimes, graph objects) to plain JSON types."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "iso_format"):  # neo4j.time types
        return value.iso_format()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    from neo4j.graph import Node, Path, Relationship
    if isinstance(value, Node):
        return {"_type": "node", "id": value.element_id, "labels": sorted(value.labels),
                "properties": jsonable(dict(value))}
    if isinstance(value, Relationship):
        return {"_type": "relationship", "id": value.element_id, "type": value.type,
                "start": value.start_node.element_id if value.start_node else None,
                "end": value.end_node.element_id if value.end_node else None,
                "properties": jsonable(dict(value))}
    if isinstance(value, Path):
        return {"_type": "path", "nodes": [jsonable(n) for n in value.nodes],
                "relationships": [jsonable(r) for r in value.relationships]}
    return str(value)


def from_mongo(doc):
    """Rename Mongo's _id to id and make the document JSON-safe."""
    if doc is None:
        return None
    result = dict(doc)
    if "_id" in result:
        result["id"] = result.pop("_id")
    return jsonable(result)
