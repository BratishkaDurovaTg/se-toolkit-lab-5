"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any

import httpx
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime string from API into Python datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)
    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected /api/items response shape")
    return payload


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs: list[dict] = []
    current_since = since.isoformat() if since is not None else None

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, Any] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            payload = response.json()

            logs = payload.get("logs", [])
            has_more = payload.get("has_more", False)
            if not isinstance(logs, list):
                raise ValueError("Unexpected /api/logs response shape")

            all_logs.extend(logs)
            if not logs or not has_more:
                break
            current_since = logs[-1]["submitted_at"]

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    created = 0
    labs_by_short_id: dict[str, ItemRecord] = {}

    labs = [item for item in items if item.get("type") == "lab"]
    tasks = [item for item in items if item.get("type") == "task"]

    for lab in labs:
        short_id = lab.get("lab")
        title = lab.get("title")
        if not short_id or not title:
            continue

        existing_lab = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == title,
            )
        )
        lab_record = existing_lab.first()
        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            await session.flush()
            created += 1
        labs_by_short_id[short_id] = lab_record

    for task in tasks:
        lab_short_id = task.get("lab")
        title = task.get("title")
        if not lab_short_id or not title:
            continue
        lab_record = labs_by_short_id.get(lab_short_id)
        if lab_record is None:
            continue

        existing_task = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == lab_record.id,
            )
        )
        if existing_task.first() is not None:
            continue

        session.add(
            ItemRecord(type="task", title=title, parent_id=lab_record.id),
        )
        created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    created = 0

    item_title_by_key: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab")
        task_short_id = item.get("task")
        title = item.get("title")
        if not lab_short_id or not title:
            continue
        key = (lab_short_id, task_short_id)
        item_title_by_key[key] = title

    for log in logs:
        student_external_id = log.get("student_id")
        if not student_external_id:
            continue

        learner_result = await session.exec(
            select(Learner).where(Learner.external_id == student_external_id)
        )
        learner = learner_result.first()
        if learner is None:
            learner = Learner(
                external_id=student_external_id,
                student_group=log.get("group", ""),
            )
            session.add(learner)
            await session.flush()

        item_title = item_title_by_key.get((log.get("lab"), log.get("task")))
        if item_title is None:
            continue
        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()
        if item is None:
            continue

        interaction_external_id = log.get("id")
        if interaction_external_id is None:
            continue
        existing_interaction = await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == interaction_external_id
            )
        )
        if existing_interaction.first() is not None:
            continue

        submitted_at = log.get("submitted_at")
        if submitted_at is None:
            continue
        session.add(
            InteractionLog(
                external_id=interaction_external_id,
                learner_id=learner.id,  # type: ignore[arg-type]
                item_id=item.id,  # type: ignore[arg-type]
                kind="attempt",
                score=log.get("score"),
                checks_passed=log.get("passed"),
                checks_total=log.get("total"),
                created_at=_parse_iso_datetime(submitted_at),
            )
        )
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    items = await fetch_items()
    await load_items(items, session)

    last_timestamp_result = await session.exec(select(func.max(InteractionLog.created_at)))
    last_timestamp = last_timestamp_result.one()

    logs = await fetch_logs(since=last_timestamp)
    new_records = await load_logs(logs, items, session)

    total_records_result = await session.exec(select(func.count()).select_from(InteractionLog))
    total_records = int(total_records_result.one())

    return {"new_records": new_records, "total_records": total_records}
