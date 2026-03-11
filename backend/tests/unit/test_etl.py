"""Unit tests for the ETL pipeline."""

from datetime import datetime

import pytest
from sqlalchemy import JSON, event
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import SQLModel, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.etl import load_items, load_logs, sync
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner


@pytest.fixture
async def engine():
    """Create an in-memory async SQLite engine with test schema."""
    from sqlalchemy.dialects.postgresql import JSONB

    @event.listens_for(SQLModel.metadata, "column_reflect")
    def _reflect(inspector, table, column_info):  # noqa: ANN001 ARG001
        if isinstance(column_info["type"], JSONB):
            column_info["type"] = JSON()

    for col in ItemRecord.__table__.columns:
        if isinstance(col.type, JSONB):
            col.type = JSON()

    eng = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture
async def session(engine):
    """Provide a database session bound to the test engine."""
    async with AsyncSession(engine) as sess:
        yield sess


@pytest.fixture
def items_catalog() -> list[dict]:
    """Sample autochecker item catalog."""
    return [
        {"lab": "lab-01", "task": None, "title": "Lab 01 - Intro", "type": "lab"},
        {
            "lab": "lab-01",
            "task": "setup",
            "title": "Repository Setup",
            "type": "task",
        },
        {
            "lab": "lab-01",
            "task": "api",
            "title": "Build API",
            "type": "task",
        },
        {"lab": "lab-02", "task": None, "title": "Lab 02 - Data", "type": "lab"},
        {
            "lab": "lab-02",
            "task": "setup",
            "title": "Repository Setup",
            "type": "task",
        },
    ]


@pytest.mark.asyncio
async def test_load_items_creates_labs_and_tasks(
    session: AsyncSession, items_catalog: list[dict]
) -> None:
    created = await load_items(items_catalog, session)

    result = await session.exec(select(ItemRecord))
    items = result.all()

    assert created == 5
    assert len(items) == 5

    lab_01 = next(item for item in items if item.title == "Lab 01 - Intro")
    lab_02 = next(item for item in items if item.title == "Lab 02 - Data")
    task_pairs = {(item.parent_id, item.title) for item in items if item.type == "task"}
    assert (lab_01.id, "Repository Setup") in task_pairs
    assert (lab_02.id, "Repository Setup") in task_pairs


@pytest.mark.asyncio
async def test_load_logs_creates_learners_and_skips_duplicate_interactions(
    session: AsyncSession, items_catalog: list[dict]
) -> None:
    await load_items(items_catalog, session)
    logs = [
        {
            "id": 101,
            "student_id": "stu-1",
            "group": "B23-CS-01",
            "lab": "lab-01",
            "task": "setup",
            "score": 100.0,
            "passed": 4,
            "total": 4,
            "submitted_at": "2026-03-01T10:00:00Z",
        },
        {
            "id": 102,
            "student_id": "stu-1",
            "group": "B23-CS-01",
            "lab": "lab-02",
            "task": "setup",
            "score": 75.0,
            "passed": 3,
            "total": 4,
            "submitted_at": "2026-03-02T11:00:00Z",
        },
        {
            "id": 101,
            "student_id": "stu-1",
            "group": "B23-CS-01",
            "lab": "lab-01",
            "task": "setup",
            "score": 100.0,
            "passed": 4,
            "total": 4,
            "submitted_at": "2026-03-01T10:00:00Z",
        },
    ]

    created = await load_logs(logs, items_catalog, session)

    learner_result = await session.exec(select(Learner))
    learners = learner_result.all()
    interaction_result = await session.exec(select(InteractionLog))
    interactions = interaction_result.all()

    assert created == 2
    assert len(learners) == 1
    assert len(interactions) == 2

    item_result = await session.exec(select(ItemRecord))
    items = item_result.all()
    lab_01 = next(item for item in items if item.title == "Lab 01 - Intro")
    lab_02 = next(item for item in items if item.title == "Lab 02 - Data")
    task_lab_01 = next(
        item
        for item in items
        if item.parent_id == lab_01.id and item.title == "Repository Setup"
    )
    task_lab_02 = next(
        item
        for item in items
        if item.parent_id == lab_02.id and item.title == "Repository Setup"
    )
    assert {interaction.item_id for interaction in interactions} == {
        task_lab_01.id,
        task_lab_02.id,
    }


@pytest.mark.asyncio
async def test_sync_returns_new_and_total_records(
    monkeypatch: pytest.MonkeyPatch,
    session: AsyncSession,
    items_catalog: list[dict],
) -> None:
    logs = [
        {
            "id": 201,
            "student_id": "stu-2",
            "group": "B23-CS-02",
            "lab": "lab-01",
            "task": "api",
            "score": 80.0,
            "passed": 4,
            "total": 5,
            "submitted_at": "2026-03-03T09:00:00Z",
        }
    ]
    observed_since: list[datetime | None] = []

    async def fake_fetch_items() -> list[dict]:
        return items_catalog

    async def fake_fetch_logs(since: datetime | None = None) -> list[dict]:
        observed_since.append(since)
        return logs

    monkeypatch.setattr("app.etl.fetch_items", fake_fetch_items)
    monkeypatch.setattr("app.etl.fetch_logs", fake_fetch_logs)

    first_result = await sync(session)
    second_result = await sync(session)

    assert first_result == {"new_records": 1, "total_records": 1}
    assert second_result == {"new_records": 0, "total_records": 1}
    assert observed_since[0] is None
    assert observed_since[1] == datetime(2026, 3, 3, 9, 0)
