"""Add usage_stats to assessments

Revision ID: 003_add_usage_stats
Revises: 002_add_complexity_fields
Create Date: 2026-02-01

Stores optional usage_stats JSON (from usage_stats.json upload) per assessment.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "003_add_usage_stats"
down_revision = "002_add_complexity_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "assessments",
        sa.Column("usage_stats", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("assessments", "usage_stats")
