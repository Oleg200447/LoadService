"""Initialize system topics

Revision ID: 002_init_system_topics
Revises: 001_initial_schema
Create Date: 2026-01-02 02:01:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '002_init_system_topics'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Insert 5 system topics
    op.execute("""
        INSERT INTO topics (name, owner_id, is_system, created_at)
        VALUES 
            ('system_1', NULL, true, now()),
            ('system_2', NULL, true, now()),
            ('system_3', NULL, true, now()),
            ('system_4', NULL, true, now()),
            ('system_5', NULL, true, now());
    """)


def downgrade() -> None:
    # Remove system topics
    op.execute("""
        DELETE FROM topics 
        WHERE name IN ('system_1', 'system_2', 'system_3', 'system_4', 'system_5')
        AND is_system = true;
    """)
