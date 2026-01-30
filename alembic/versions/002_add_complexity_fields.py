"""Add complexity and hierarchy fields

Revision ID: 002_add_complexity_fields
Revises: 001_initial
Create Date: 2026-01-23 12:00:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002_add_complexity_fields'
down_revision = '001_initial'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Helper function to check if column exists
    def column_exists(table_name, column_name):
        bind = op.get_bind()
        inspector = sa.inspect(bind)
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        return column_name in columns
    
    def index_exists(table_name, index_name):
        bind = op.get_bind()
        inspector = sa.inspect(bind)
        indexes = [idx['name'] for idx in inspector.get_indexes(table_name)]
        return index_name in indexes
    
    def table_exists(table_name):
        bind = op.get_bind()
        inspector = sa.inspect(bind)
        return table_name in inspector.get_table_names()
    
    # Add complexity fields to extracted_objects (only if they don't exist)
    if not column_exists('extracted_objects', 'complexity_score_looker'):
        op.add_column('extracted_objects', sa.Column('complexity_score_looker', sa.Float(), nullable=True))
    if not column_exists('extracted_objects', 'complexity_level_looker'):
        op.add_column('extracted_objects', sa.Column('complexity_level_looker', sa.String(20), nullable=True))
    if not column_exists('extracted_objects', 'complexity_score_custom'):
        op.add_column('extracted_objects', sa.Column('complexity_score_custom', sa.Float(), nullable=True))
    if not column_exists('extracted_objects', 'complexity_level_custom'):
        op.add_column('extracted_objects', sa.Column('complexity_level_custom', sa.String(20), nullable=True))
    if not column_exists('extracted_objects', 'hierarchy_depth'):
        op.add_column('extracted_objects', sa.Column('hierarchy_depth', sa.Integer(), nullable=True))
    if not column_exists('extracted_objects', 'hierarchy_level'):
        op.add_column('extracted_objects', sa.Column('hierarchy_level', sa.Integer(), nullable=True))
    if not column_exists('extracted_objects', 'hierarchy_path'):
        op.add_column('extracted_objects', sa.Column('hierarchy_path', sa.Text(), nullable=True))
    
    # Add indexes for complexity queries (only if they don't exist)
    if not index_exists('extracted_objects', 'ix_extracted_objects_complexity_looker'):
        op.create_index('ix_extracted_objects_complexity_looker', 'extracted_objects', ['complexity_level_looker'])
    if not index_exists('extracted_objects', 'ix_extracted_objects_complexity_custom'):
        op.create_index('ix_extracted_objects_complexity_custom', 'extracted_objects', ['complexity_level_custom'])
    if not index_exists('extracted_objects', 'ix_extracted_objects_hierarchy_depth'):
        op.create_index('ix_extracted_objects_hierarchy_depth', 'extracted_objects', ['hierarchy_depth'])
    
    # Add complexity fields to object_relationships (only if they don't exist)
    if not column_exists('object_relationships', 'complexity_score'):
        op.add_column('object_relationships', sa.Column('complexity_score', sa.Float(), nullable=True))
    if not column_exists('object_relationships', 'complexity_level'):
        op.add_column('object_relationships', sa.Column('complexity_level', sa.String(20), nullable=True))
    
    # Create complexity_config table (only if it doesn't exist)
    if not table_exists('complexity_config'):
        op.create_table(
            'complexity_config',
            sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('assessment_id', postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column('mode', sa.String(20), nullable=False),  # 'looker' or 'custom'
            sa.Column('config', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.Column('updated_at', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index('ix_complexity_config_assessment', 'complexity_config', ['assessment_id'])
        op.create_index('ix_complexity_config_mode', 'complexity_config', ['mode'])
    
    # Create custom_complexity_mapping table (only if it doesn't exist)
    if not table_exists('custom_complexity_mapping'):
        op.create_table(
            'custom_complexity_mapping',
            sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column('assessment_id', postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column('feature_area', sa.String(100), nullable=True),
            sa.Column('feature', sa.String(200), nullable=False),
            sa.Column('complexity', sa.String(20), nullable=False),  # Low, Medium, High, Critical
            sa.Column('feasibility', sa.String(20), nullable=True),  # Yes, No, Partial
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('recommended_approach', sa.Text(), nullable=True),
            sa.Column('created_at', sa.DateTime(), nullable=True),
            sa.Column('updated_at', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index('ix_custom_mapping_assessment', 'custom_complexity_mapping', ['assessment_id'])
        op.create_index('ix_custom_mapping_feature', 'custom_complexity_mapping', ['feature'])


def downgrade() -> None:
    op.drop_index('ix_custom_mapping_feature', table_name='custom_complexity_mapping')
    op.drop_index('ix_custom_mapping_assessment', table_name='custom_complexity_mapping')
    op.drop_table('custom_complexity_mapping')
    op.drop_index('ix_complexity_config_mode', table_name='complexity_config')
    op.drop_index('ix_complexity_config_assessment', table_name='complexity_config')
    op.drop_table('complexity_config')
    op.drop_column('object_relationships', 'complexity_level')
    op.drop_column('object_relationships', 'complexity_score')
    op.drop_index('ix_extracted_objects_hierarchy_depth', table_name='extracted_objects')
    op.drop_index('ix_extracted_objects_complexity_custom', table_name='extracted_objects')
    op.drop_index('ix_extracted_objects_complexity_looker', table_name='extracted_objects')
    op.drop_column('extracted_objects', 'hierarchy_path')
    op.drop_column('extracted_objects', 'hierarchy_level')
    op.drop_column('extracted_objects', 'hierarchy_depth')
    op.drop_column('extracted_objects', 'complexity_level_custom')
    op.drop_column('extracted_objects', 'complexity_score_custom')
    op.drop_column('extracted_objects', 'complexity_level_looker')
    op.drop_column('extracted_objects', 'complexity_score_looker')
