"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from typing import Any, Literal, NotRequired, TypedDict, cast

import httpx
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


class AutocheckerItem(TypedDict):
    """Raw item payload returned by the autochecker API."""

    lab: str
    task: str | None
    title: str
    type: Literal["lab", "task"]


class AutocheckerLog(TypedDict):
    """Raw log payload returned by the autochecker API."""

    id: int
    student_id: str
    group: str
    lab: str
    task: str | None
    score: NotRequired[float | None]
    passed: NotRequired[int | None]
    total: NotRequired[int | None]
    submitted_at: str


class AutocheckerLogsResponse(TypedDict):
    """Paginated autochecker logs response."""

    logs: list[AutocheckerLog]
    count: int
    has_more: bool


class SyncSummary(TypedDict):
    """Return payload for the sync endpoint."""

    new_records: int
    total_records: int


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime string from API into Python datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _get_autochecker_auth() -> httpx.BasicAuth:
    """Build HTTP Basic Auth credentials for the autochecker API."""
    return httpx.BasicAuth(
        username=settings.autochecker_email,
        password=settings.autochecker_password,
    )


async def fetch_items() -> list[AutocheckerItem]:
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
    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=_get_autochecker_auth())
        response.raise_for_status()
    payload = cast(object, response.json())
    if not isinstance(payload, list):
        raise ValueError("Unexpected /api/items response shape")
    return cast(list[AutocheckerItem], payload)


async def fetch_logs(since: datetime | None = None) -> list[AutocheckerLog]:
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
    all_logs: list[AutocheckerLog] = []
    current_since = since.isoformat() if since is not None else None

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, Any] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since

            response = await client.get(
                url, auth=_get_autochecker_auth(), params=params
            )
            response.raise_for_status()
            payload = cast(object, response.json())
            if not isinstance(payload, dict):
                raise ValueError("Unexpected /api/logs response shape")
            typed_payload = cast(AutocheckerLogsResponse, payload)

            logs = typed_payload["logs"]
            has_more = typed_payload["has_more"]

            all_logs.extend(logs)
            if not logs or not has_more:
                break

            next_since = logs[-1]["submitted_at"]
            if next_since == current_since:
                break
            current_since = next_since

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[AutocheckerItem], session: AsyncSession) -> int:
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

    labs = [item for item in items if item["type"] == "lab"]
    tasks = [item for item in items if item["type"] == "task"]

    for lab in labs:
        short_id = lab["lab"]
        title = lab["title"]

        existing_lab_result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == title,
            )
        )
        lab_record = existing_lab_result.first()
        if lab_record is None:
            lab_record = ItemRecord(type="lab", title=title)
            session.add(lab_record)
            await session.flush()
            created += 1
        labs_by_short_id[short_id] = lab_record

    for task in tasks:
        lab_short_id = task["lab"]
        title = task["title"]
        lab_record = labs_by_short_id.get(lab_short_id)
        if lab_record is None:
            continue

        existing_task_result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == lab_record.id,
            )
        )
        if existing_task_result.first() is not None:
            continue

        session.add(
            ItemRecord(type="task", title=title, parent_id=lab_record.id),
        )
        created += 1

    await session.commit()
    return created


async def load_logs(
    logs: list[AutocheckerLog],
    items_catalog: list[AutocheckerItem],
    session: AsyncSession,
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
        lab_short_id = item["lab"]
        task_short_id = item["task"]
        title = item["title"]
        key = (lab_short_id, task_short_id)
        item_title_by_key[key] = title

    item_result = await session.exec(select(ItemRecord))
    db_items = item_result.all()
    db_labs_by_title = {
        item.title: item for item in db_items if item.type == "lab" and item.id is not None
    }
    db_tasks_by_parent_and_title = {
        (item.parent_id, item.title): item
        for item in db_items
        if item.type == "task" and item.parent_id is not None and item.id is not None
    }

    learner_result = await session.exec(select(Learner))
    learners_by_external_id = {
        learner.external_id: learner for learner in learner_result.all()
    }

    interaction_result = await session.exec(select(InteractionLog.external_id))
    existing_interaction_ids = {
        interaction_id
        for interaction_id in interaction_result.all()
        if interaction_id is not None
    }

    for log in logs:
        student_external_id = log["student_id"]

        learner = learners_by_external_id.get(student_external_id)
        if learner is None:
            learner = Learner(
                external_id=student_external_id,
                student_group=log["group"],
            )
            session.add(learner)
            await session.flush()
            learners_by_external_id[student_external_id] = learner

        lab_short_id = log["lab"]
        task_short_id = log["task"]
        lab_title = item_title_by_key.get((lab_short_id, None))
        if lab_title is None:
            continue
        lab_item = db_labs_by_title.get(lab_title)
        if lab_item is None or lab_item.id is None:
            continue

        item_title = item_title_by_key.get((lab_short_id, task_short_id))
        if item_title is None:
            item = lab_item
        else:
            item = db_tasks_by_parent_and_title.get((lab_item.id, item_title))
        if item is None:
            continue

        interaction_external_id = log["id"]
        if interaction_external_id in existing_interaction_ids:
            continue

        submitted_at = log["submitted_at"]
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
        existing_interaction_ids.add(interaction_external_id)
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> SyncSummary:
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

    last_timestamp_result = await session.exec(
        select(func.max(InteractionLog.created_at))
    )
    last_timestamp = last_timestamp_result.one()

    logs = await fetch_logs(since=last_timestamp)
    new_records = await load_logs(logs, items, session)

    total_records_result = await session.exec(select(func.count()).select_from(InteractionLog))
    total_records = int(total_records_result.one())

    return {"new_records": new_records, "total_records": total_records}
