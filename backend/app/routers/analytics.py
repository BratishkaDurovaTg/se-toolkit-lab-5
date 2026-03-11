"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from collections.abc import Sequence

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case
from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


def _lab_title_fragment(lab: str) -> str:
    """Convert a short lab id like 'lab-04' into a title fragment."""
    return lab.replace("lab-", "Lab ").replace("-", " ")


async def _get_lab_task_ids(session: AsyncSession, lab: str) -> list[int]:
    """Return task item ids that belong to the requested lab."""
    lab_title = _lab_title_fragment(lab)
    lab_result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(lab_title),
        )
    )
    lab_item = lab_result.first()
    if lab_item is None or lab_item.id is None:
        return []

    task_result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    return [task_id for task_id in task_result.all() if task_id is not None]


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    task_ids = await _get_lab_task_ids(session, lab)
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    bucket_case = case(
        (col(InteractionLog.score) <= 25, "0-25"),
        (col(InteractionLog.score) <= 50, "26-50"),
        (col(InteractionLog.score) <= 75, "51-75"),
        else_="76-100",
    )

    result = await session.exec(
        select(bucket_case.label("bucket"), func.count())
        .where(
            col(InteractionLog.item_id).in_(task_ids),
            col(InteractionLog.score).is_not(None),
        )
        .group_by(bucket_case)
    )
    counts_by_bucket = {bucket: count for bucket, count in result.all()}

    ordered_buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [
        {"bucket": bucket, "count": int(counts_by_bucket.get(bucket, 0))}
        for bucket in ordered_buckets
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    task_ids = await _get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            ItemRecord.title,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(InteractionLog.id),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(col(ItemRecord.id).in_(task_ids))
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    return [
        {"task": title, "avg_score": float(avg_score), "attempts": int(attempts)}
        for title, avg_score, attempts in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    task_ids = await _get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    day_expr = func.date(InteractionLog.created_at)
    result = await session.exec(
        select(day_expr.label("day"), func.count(InteractionLog.id))
        .where(col(InteractionLog.item_id).in_(task_ids))
        .group_by(day_expr)
        .order_by(day_expr)
    )
    return [
        {"date": str(day), "submissions": int(submissions)}
        for day, submissions in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    task_ids = await _get_lab_task_ids(session, lab)
    if not task_ids:
        return []

    result = await session.exec(
        select(
            Learner.student_group,
            func.round(func.avg(InteractionLog.score), 1),
            func.count(func.distinct(Learner.id)),
        )
        .join(Learner, Learner.id == InteractionLog.learner_id)
        .where(col(InteractionLog.item_id).in_(task_ids))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    return [
        {
            "group": student_group,
            "avg_score": float(avg_score),
            "students": int(students),
        }
        for student_group, avg_score, students in result.all()
    ]
