"""Add state, latest_source, file_created_at, meta_info refactor, import_logs enhancement

Revision ID: a1b2c3d4e5f6
Revises: 4e501d78b124
Create Date: 2026-04-07

This migration:
1. Adds new columns to leads: state, latest_source, latest_campaign,
   file_created_at, import_count, brevo_id
2. Renames metadata -> meta_info (adds meta_info, copies data, drops metadata)
3. Enhances import_logs with status, error_details, imported_at
4. Adds digest_recipients.full_name
5. Creates dashboard_permissions table
6. Adds indexes for performance
7. Normalizes existing data (city Title Case, country uppercase)
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers
revision = 'a1b2c3d4e5f6'
down_revision = '4e501d78b124'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── 1. Add new columns to leads ───────────────────────────────

    # Check and add columns (idempotent with IF NOT EXISTS via raw SQL)
    op.execute("""
        DO $$
        BEGIN
            -- state column
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'state'
            ) THEN
                ALTER TABLE leads ADD COLUMN state VARCHAR(10);
            END IF;

            -- latest_source
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'latest_source'
            ) THEN
                ALTER TABLE leads ADD COLUMN latest_source VARCHAR(255);
            END IF;

            -- latest_campaign
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'latest_campaign'
            ) THEN
                ALTER TABLE leads ADD COLUMN latest_campaign VARCHAR(255);
            END IF;

            -- file_created_at
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'file_created_at'
            ) THEN
                ALTER TABLE leads ADD COLUMN file_created_at TIMESTAMPTZ;
            END IF;

            -- import_count
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'import_count'
            ) THEN
                ALTER TABLE leads ADD COLUMN import_count INTEGER DEFAULT 1;
            END IF;

            -- brevo_id
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'brevo_id'
            ) THEN
                ALTER TABLE leads ADD COLUMN brevo_id VARCHAR(100);
            END IF;

            -- meta_info (new name for metadata)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'meta_info'
            ) THEN
                ALTER TABLE leads ADD COLUMN meta_info JSONB DEFAULT '{}'::jsonb;
            END IF;
        END $$;
    """)

    # ── 2. Copy metadata to meta_info if metadata column exists ────
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'metadata'
            ) THEN
                UPDATE leads SET meta_info = metadata WHERE meta_info IS NULL OR meta_info = '{}'::jsonb;
                ALTER TABLE leads DROP COLUMN metadata;
            END IF;
        END $$;
    """)

    # ── 3. Backfill latest_source from source ──────────────────────
    op.execute("""
        UPDATE leads
        SET latest_source = source
        WHERE latest_source IS NULL AND source IS NOT NULL;
    """)

    # ── 4. Enhance import_logs ─────────────────────────────────────
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'import_logs' AND column_name = 'status'
            ) THEN
                ALTER TABLE import_logs ADD COLUMN status TEXT DEFAULT 'success';
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'import_logs' AND column_name = 'error_details'
            ) THEN
                ALTER TABLE import_logs ADD COLUMN error_details JSONB;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'import_logs' AND column_name = 'imported_at'
            ) THEN
                ALTER TABLE import_logs ADD COLUMN imported_at TIMESTAMPTZ DEFAULT NOW();
            END IF;
        END $$;
    """)

    # ── 5. Add full_name to digest_recipients ──────────────────────
    # ── 5. Add full_name to digest_recipients ──────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS digest_recipients (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            full_name VARCHAR(255),
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'digest_recipients' AND column_name = 'full_name'
            ) THEN
                ALTER TABLE digest_recipients ADD COLUMN full_name VARCHAR(255);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'digest_recipients' AND column_name = 'created_at'
            ) THEN
                ALTER TABLE digest_recipients ADD COLUMN created_at TIMESTAMPTZ DEFAULT NOW();
            END IF;
        END $$;
    """)

    # ── 6. Create dashboard_permissions table ──────────────────────
    op.execute("DROP TABLE IF EXISTS dashboard_permissions;")
    op.execute("""
        CREATE TABLE dashboard_permissions (
            id SERIAL PRIMARY KEY,
            role TEXT NOT NULL UNIQUE,
            dashboard_ids INTEGER[] NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    op.execute("""
        INSERT INTO dashboard_permissions (role, dashboard_ids) VALUES
            ('admin',   ARRAY[15, 16, 17]),
            ('manager', ARRAY[15, 16]),
            ('viewer',  ARRAY[15])
        ON CONFLICT (role) DO NOTHING;
    """)

    # ── 7. Add indexes ─────────────────────────────────────────────
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_leads_email ON leads (email);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_country ON leads (country_iso2);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_city ON leads (city);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_source ON leads (source);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_latest_source ON leads (latest_source);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_status ON leads (status);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_created_at ON leads (created_at);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_state ON leads (state);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_is_buyer ON leads (is_buyer);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_meta_info_gin ON leads USING GIN (meta_info);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_leads_tags_gin ON leads USING GIN (tags);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_import_logs_imported_at ON import_logs (imported_at);")

    # ── 8. Normalize existing data ─────────────────────────────────

    # 8a. City: normalize to Title Case (INITCAP)
    op.execute("""
        UPDATE leads
        SET city = INITCAP(LOWER(TRIM(city)))
        WHERE city IS NOT NULL
          AND city != ''
          AND city != INITCAP(LOWER(TRIM(city)));
    """)

    # 8b. Country: normalize to uppercase 2-letter codes
    op.execute("""
        UPDATE leads
        SET country_iso2 = UPPER(TRIM(country_iso2))
        WHERE country_iso2 IS NOT NULL
          AND country_iso2 != ''
          AND country_iso2 != UPPER(TRIM(country_iso2));
    """)

    # 8c. Email: normalize to lowercase
    op.execute("""
        UPDATE leads
        SET email = LOWER(TRIM(email))
        WHERE email != LOWER(TRIM(email));
    """)

    # 8d. Deduplicate leads by email (keep the one with lowest id)
    # First, merge phones arrays for duplicates
    op.execute("""
        -- Remove exact email duplicates (keep lowest id)
        DELETE FROM leads a
        USING leads b
        WHERE a.id > b.id
          AND LOWER(TRIM(a.email)) = LOWER(TRIM(b.email));
    """)

    # 8e. Fix NULL phones to empty array
    op.execute("UPDATE leads SET phones = '[]'::jsonb WHERE phones IS NULL;")
    op.execute("UPDATE leads SET tags = '[]'::jsonb WHERE tags IS NULL;")
    op.execute("UPDATE leads SET meta_info = '{}'::jsonb WHERE meta_info IS NULL;")

    # 8f. Set default import_count for existing records
    op.execute("""
        UPDATE leads SET import_count = 1 WHERE import_count IS NULL;
    """)

    # 8g. Clean up NOT_CITIES in city field
    op.execute("""
        UPDATE leads
        SET city = NULL
        WHERE LOWER(TRIM(city)) IN (
            'europe', 'asia', 'africa', 'oceania', 'americas',
            'middle east', 'north america', 'south america',
            'usa', 'us', 'u.s.', 'u.s.a.', 'united states', 'america',
            'uk', 'u.k.', 'united kingdom', 'great britain', 'england',
            'canada', 'australia', 'new zealand',
            'germany', 'france', 'italy', 'spain', 'netherlands',
            'n/a', 'na', 'none', 'null', 'unknown', '-', '--', '.',
            'test', 'testing', 'other', 'general', 'various',
            'florida general', 'florida', 'california', 'texas'
        );
    """)


def downgrade() -> None:
    # Remove new columns (reversible)
    op.execute("""
        DO $$
        BEGIN
            -- Add back metadata from meta_info
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'leads' AND column_name = 'metadata'
            ) THEN
                ALTER TABLE leads ADD COLUMN metadata JSONB DEFAULT '{}'::jsonb;
                UPDATE leads SET metadata = meta_info;
            END IF;
        END $$;
    """)

    # Drop new columns
    for col in ['state', 'latest_source', 'latest_campaign', 'file_created_at',
                'import_count', 'brevo_id', 'meta_info']:
        op.execute(f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'leads' AND column_name = '{col}'
                ) THEN
                    ALTER TABLE leads DROP COLUMN {col};
                END IF;
            END $$;
        """)

    # Drop new indexes
    op.execute("DROP INDEX IF EXISTS ix_leads_latest_source;")
    op.execute("DROP INDEX IF EXISTS ix_leads_state;")
    op.execute("DROP INDEX IF EXISTS ix_leads_is_buyer;")
    op.execute("DROP INDEX IF EXISTS ix_leads_meta_info_gin;")
    op.execute("DROP INDEX IF EXISTS ix_leads_tags_gin;")

    # Drop dashboard_permissions table
    op.execute("DROP TABLE IF EXISTS dashboard_permissions;")
