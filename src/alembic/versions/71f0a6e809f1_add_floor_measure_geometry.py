"""Add floor measure geometry

Revision ID: 71f0a6e809f1
Revises: 88f6a3295610
Create Date: 2025-06-11 01:04:43.800202

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = '71f0a6e809f1'
down_revision: Union[str, None] = '88f6a3295610'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('floor_measure', sa.Column('location', geoalchemy2.types.Geometry(geometry_type='POINT', srid=7844, from_text='ST_GeomFromEWKT', name='geometry', nullable=True, spatial_index=False), nullable=True))
    op.create_index('idx_floor_measure_location', 'floor_measure', ['location'], unique=False, postgresql_using='gist')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('idx_floor_measure_location', table_name='floor_measure', postgresql_using='gist')
    op.drop_column('floor_measure', 'location')
    # ### end Alembic commands ###
