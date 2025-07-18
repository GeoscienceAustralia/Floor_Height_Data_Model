"""Generate UUIDs for all tables

Revision ID: b769c250b877
Revises: 71f0a6e809f1
Create Date: 2025-06-18 01:33:02.707225

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = "b769c250b877"
down_revision: Union[str, None] = "71f0a6e809f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "floor_measure",
        "id",
        server_default=sa.text("gen_random_uuid()"),
    )
    op.alter_column(
        "floor_measure_image",
        "id",
        server_default=sa.text("gen_random_uuid()"),
    )
    op.alter_column(
        "method",
        "id",
        server_default=sa.text("gen_random_uuid()"),
    )
    op.alter_column(
        "dataset",
        "id",
        server_default=sa.text("gen_random_uuid()"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column(
        "floor_measure",
        "id",
        server_default=None,
    )
    op.alter_column(
        "floor_measure_image",
        "id",
        server_default=None,
    )
    op.alter_column(
        "method",
        "id",
        server_default=None,
    )
    op.alter_column(
        "dataset",
        "id",
        server_default=None,
    )
    # ### end Alembic commands ###
