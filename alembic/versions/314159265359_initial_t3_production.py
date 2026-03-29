"""Initial T3 Production Migration

Revision ID: 314159265359
Revises: 
Create Date: 2026-03-29 05:05:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '314159265359'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # 1. nationality VARCHAR(100) (Part 9)
    op.alter_column('leads', 'nationality', type_=sa.String(length=100))
    
    # 2. GIN Index (Part 9)
    # Check if index exists is best done with a try-except or raw SQL
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_metadata_gin ON leads USING gin (metadata)")

def downgrade():
    op.drop_index('ix_leads_metadata_gin', table_name='leads')
