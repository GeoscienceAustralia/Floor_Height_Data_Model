"""Change cardinality of floor_measure and floor_measure_image to many-to-many

Revision ID: 9434c9068d84
Revises: 1af3ceff9a62
Create Date: 2025-06-23 05:58:01.822030

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = '9434c9068d84'
down_revision: Union[str, None] = '1af3ceff9a62'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('floor_measure_floor_measure_image_association',
    sa.Column('floor_measure_id', sa.UUID(), nullable=False),
    sa.Column('floor_measure_image_id', sa.UUID(), nullable=False),
    sa.ForeignKeyConstraint(['floor_measure_id'], ['floor_measure.id'], ),
    sa.ForeignKeyConstraint(['floor_measure_image_id'], ['floor_measure_image.id'], ),
    sa.PrimaryKeyConstraint('floor_measure_id', 'floor_measure_image_id')
    )
    op.drop_constraint('floor_measure_image_floor_measure_id_fkey', 'floor_measure_image', type_='foreignkey')
    op.drop_column('floor_measure_image', 'floor_measure_id')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('floor_measure_image', sa.Column('floor_measure_id', sa.UUID(), autoincrement=False, nullable=False))
    op.create_foreign_key('floor_measure_image_floor_measure_id_fkey', 'floor_measure_image', 'floor_measure', ['floor_measure_id'], ['id'])
    op.drop_table('floor_measure_floor_measure_image_association')
    # ### end Alembic commands ###
