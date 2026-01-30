"""Initial migration

Revision ID: 001_initial
Revises: 
Create Date: 2026-01-22 20:58:00

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001_initial'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create users table
    op.create_table(
        'users',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('email', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('is_guest', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_users_email'), 'users', ['email'], unique=True)

    # Create assessments table
    op.create_table(
        'assessments',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('bi_tool', sa.String(), nullable=True),
        sa.Column('status', sa.Enum('CREATED', 'PROCESSING', 'COMPLETED', 'PARTIAL', 'FAILED', name='assessmentstatus'), nullable=True),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create uploaded_files table
    op.create_table(
        'uploaded_files',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('assessment_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('filename', sa.String(), nullable=False),
        sa.Column('file_path', sa.String(), nullable=False),
        sa.Column('file_type', sa.Enum('ZIP', 'XML', 'JSON', name='filetype'), nullable=False),
        sa.Column('file_size', sa.Integer(), nullable=False),
        sa.Column('parse_status', sa.Enum('PENDING', 'PARSING', 'COMPLETED', 'PARTIAL', 'FAILED', name='parsestatus'), nullable=True),
        sa.Column('uploaded_at', sa.DateTime(), nullable=True),
        sa.Column('parsed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id'], ),
        sa.PrimaryKeyConstraint('id')
    )

    # Create extracted_objects table
    op.create_table(
        'extracted_objects',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('assessment_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('object_type', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('path', sa.String(), nullable=True),
        sa.Column('properties', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('raw_xml', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id'], ),
        sa.ForeignKeyConstraint(['file_id'], ['uploaded_files.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_extracted_objects_name_search'), 'extracted_objects', ['name'])
    op.create_index(op.f('ix_extracted_objects_object_type'), 'extracted_objects', ['object_type'])
    op.create_index('ix_extracted_objects_assessment_type', 'extracted_objects', ['assessment_id', 'object_type'])

    # Create object_relationships table
    op.create_table(
        'object_relationships',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('assessment_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source_object_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('target_object_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('relationship_type', sa.String(), nullable=False),
        sa.Column('details', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id'], ),
        sa.ForeignKeyConstraint(['source_object_id'], ['extracted_objects.id'], ),
        sa.ForeignKeyConstraint(['target_object_id'], ['extracted_objects.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_relationships_assessment', 'object_relationships', ['assessment_id'])
    op.create_index('ix_relationships_source', 'object_relationships', ['source_object_id'])
    op.create_index('ix_relationships_target', 'object_relationships', ['target_object_id'])

    # Create parse_errors table
    op.create_table(
        'parse_errors',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('error_type', sa.String(), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=False),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('context', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['file_id'], ['uploaded_files.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    op.drop_table('parse_errors')
    op.drop_index('ix_relationships_target', table_name='object_relationships')
    op.drop_index('ix_relationships_source', table_name='object_relationships')
    op.drop_index('ix_relationships_assessment', table_name='object_relationships')
    op.drop_table('object_relationships')
    op.drop_index('ix_extracted_objects_assessment_type', table_name='extracted_objects')
    op.drop_index(op.f('ix_extracted_objects_object_type'), table_name='extracted_objects')
    op.drop_index(op.f('ix_extracted_objects_name_search'), table_name='extracted_objects')
    op.drop_table('extracted_objects')
    op.drop_table('uploaded_files')
    op.drop_table('assessments')
    op.drop_index(op.f('ix_users_email'), table_name='users')
    op.drop_table('users')
