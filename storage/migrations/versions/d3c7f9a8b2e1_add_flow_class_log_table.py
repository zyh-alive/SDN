"""add flow_class_log table

Revision ID: d3c7f9a8b2e1
Revises: f94e85c3dd60
Create Date: 2026-05-14 17:28:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd3c7f9a8b2e1'
down_revision: Union[str, Sequence[str], None] = 'f94e85c3dd60'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('flow_class_log',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False, comment='自增主键'),
    sa.Column('flow_key', sa.String(length=128), nullable=False, comment='五元组 flow_key: src_ip:dst_ip:protocol:src_port:dst_port'),
    sa.Column('src_ip', sa.String(length=45), nullable=False, comment='源 IP'),
    sa.Column('dst_ip', sa.String(length=45), nullable=False, comment='目的 IP'),
    sa.Column('src_port', sa.Integer(), nullable=False, comment='源端口'),
    sa.Column('dst_port', sa.Integer(), nullable=False, comment='目的端口'),
    sa.Column('protocol', sa.Integer(), nullable=False, comment='IP 协议号 (6=TCP, 17=UDP)'),
    sa.Column('avg_packet_size', sa.Float(), nullable=True, comment='平均包大小 (bytes)'),
    sa.Column('avg_iat', sa.Float(), nullable=True, comment='平均包间隔 (ms)'),
    sa.Column('packet_count', sa.Integer(), nullable=True, comment='包计数'),
    sa.Column('predicted_class', sa.String(length=32), nullable=False, comment='预测类别: realtime/interactive/streaming/bulk/other'),
    sa.Column('confidence', sa.Float(), nullable=True, comment='置信度 [0, 1]'),
    sa.Column('is_pseudo_label', sa.Integer(), nullable=True, comment='是否为伪标签 (0=训练模型预测, 1=冷启动伪标签)'),
    sa.Column('timestamp', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False, comment='记录时间'),
    sa.PrimaryKeyConstraint('id', name='pk_flow_class_log'),
    mysql_charset='utf8mb4',
    mysql_engine='InnoDB'
    )
    op.create_index('idx_flow_key', 'flow_class_log', ['flow_key'], unique=False)
    op.create_index('idx_ts', 'flow_class_log', ['timestamp'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('idx_ts', table_name='flow_class_log')
    op.drop_index('idx_flow_key', table_name='flow_class_log')
    op.drop_table('flow_class_log')
