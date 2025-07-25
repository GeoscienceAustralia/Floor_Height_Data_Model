"""Include address geocode type to address_point table and make gnaf_id not nullable

Revision ID: c98909de550a
Revises: a8654f1d1d7a
Create Date: 2024-11-27 07:28:09.305568

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = 'c98909de550a'
down_revision: Union[str, None] = 'a8654f1d1d7a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('address_point', sa.Column('geocode_type', sa.String(), nullable=True))
    op.alter_column('address_point', 'gnaf_id',
               existing_type=sa.VARCHAR(length=15),
               nullable=False)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('address_point', 'gnaf_id',
               existing_type=sa.VARCHAR(length=15),
               nullable=True)
    op.drop_column('address_point', 'geocode_type')
    # ### end Alembic commands ###
