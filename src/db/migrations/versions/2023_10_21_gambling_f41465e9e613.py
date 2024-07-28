"""gambling

Revision ID: f41465e9e613
Revises: 841bfcff275a
Create Date: 2023-10-21 21:18:20.964649

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f41465e9e613"
down_revision = "841bfcff275a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "bet_matches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("match_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(
            ["match_id"],
            ["matches.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "bet_coefficients",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("bet_match_id", sa.Integer(), nullable=False),
        sa.Column("bet_type", postgresql.ENUM("T1_WIN", "T2_WIN", name="bettype"), nullable=False),
        sa.Column("coefficient", sa.DECIMAL(precision=5, scale=2), nullable=False),
        sa.ForeignKeyConstraint(
            ["bet_match_id"],
            ["bet_matches.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "bet_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("state", postgresql.ENUM("OPEN", "CLOSED", name="betstate"), nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=True),
        sa.Column("bet_type", postgresql.ENUM("T1_WIN", "T2_WIN", name="bettype"), nullable=False),
        sa.Column("bet_match_id", sa.Integer(), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("bet_coef_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(
            ["bet_coef_id"],
            ["bet_coefficients.id"],
        ),
        sa.ForeignKeyConstraint(
            ["bet_match_id"],
            ["bet_matches.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "bet_transactions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("event", postgresql.ENUM("DIRECT", "PAYOUT", name="transactionevent"), nullable=False),
        sa.Column("member_id", sa.Integer(), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("bet_event_id", sa.UUID(), nullable=True),
        sa.ForeignKeyConstraint(
            ["bet_event_id"],
            ["bet_events.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("bet_transactions")
    op.drop_table("bet_events")
    op.drop_table("bet_coefficients")
    op.drop_table("bet_matches")
    # ### end Alembic commands ###