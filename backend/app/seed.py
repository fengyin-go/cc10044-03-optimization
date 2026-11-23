"""示例数据：每个模块给多条不同状态的记录，覆盖真实湖仓场景。

流水线定义、分区配置、质量规则等模块的数据反映实际的
数据工程业务状态：命名采用真实表名/路径/时间范围，
便于下游出题和人工复审时对照理解。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any


def _order_partition_inventory() -> list[dict[str, Any]]:
    """ods_orders 订单分区台账：2025-10-01 起每日一个分区。

    预置三种状态模拟历史处置痕迹，供验证幂等（已完成不重复执行）：
    - 2026-07-20 / 2026-07-21 已转冷（上次例行清理完成，副本在冷层）
    - 2025-10-05 已物理删除（超出 365 天合规保留期）
    其余分区仍在热层待处置；其中 2026-07-26 已保存 120 天、
    2025-10-19 已保存 400 天，分别命中转冷和物理删除场景。
    """
    today = date(2026, 11, 23)
    pre_cold = {date(2026, 7, 20), date(2026, 7, 21)}
    pre_deleted = {date(2025, 10, 5)}
    rows: list[dict[str, Any]] = []
    day = date(2025, 10, 1)
    partition_id = 1
    while day < today:
        day_str = day.isoformat()
        ordinal = day.toordinal()
        row: dict[str, Any] = {
            "id": partition_id,
            "数据集": "ods_orders",
            "分区日期": day_str,
            "行数": 138000 + (ordinal * 7919) % 24000,
            "大小MB": 420 + (ordinal * 31) % 90,
            "存储层": "热层",
            "副本位置": f"/data/hot/ods_orders/dt={day_str}",
            "处置状态": "未处置",
            "处置时间": "",
            "处置作业": "",
        }
        if day in pre_cold:
            row.update({
                "存储层": "冷层",
                "副本位置": f"s3://lake-cold/ods_orders/dt={day_str}",
                "处置状态": "已转冷",
                "处置时间": "2026-11-20T02:00:00Z",
                "处置作业": "cleanup_expired_ods_partitions",
            })
        elif day in pre_deleted:
            row.update({
                "存储层": "已删除",
                "副本位置": "",
                "处置状态": "已删除",
                "处置时间": "2026-11-20T02:00:00Z",
                "处置作业": "cleanup_expired_ods_partitions",
            })
        rows.append(row)
        partition_id += 1
        day += timedelta(days=1)
    return rows


SEED_ROWS: dict[str, list[dict[str, Any]]] = {
    "etl_pipeline": [
        {"id": 1, "status": "已启用", "pending": False, "abnormal": False,
         "流水线名称": "ods_order_inc_daily", "源端类型": "MySQL", "目标端类型": "Hive",
         "执行周期": "每天 02:00", "最近运行状态": "成功"},
        {"id": 2, "status": "运行中", "pending": True, "abnormal": False,
         "流水线名称": "dwd_user_event_cdc", "源端类型": "Kafka", "目标端类型": "Iceberg",
         "执行周期": "持续运行", "最近运行状态": "运行中"},
        {"id": 3, "status": "草稿", "pending": True, "abnormal": False,
         "流水线名称": "ods_inventory_full_monthly", "源端类型": "PostgreSQL", "目标端类型": "Parquet",
         "执行周期": "每月 1日 03:00", "最近运行状态": "未运行"},
        {"id": 4, "status": "已停用", "pending": False, "abnormal": True,
         "流水线名称": "legacy_mysql_dump_daily", "源端类型": "MySQL", "目标端类型": "HDFS",
         "执行周期": "每天 01:00", "最近运行状态": "超时终止"},
        {"id": 5, "status": "已启用", "pending": False, "abnormal": False,
         "流水线名称": "dws_sales_agg_hourly", "源端类型": "Hive", "目标端类型": "ClickHouse",
         "执行周期": "每小时", "最近运行状态": "成功"},
    ],
    "partition_manager": [
        {"id": 1, "status": "已启用", "pending": False, "abnormal": False,
         "配置名称": "ods_order_dt_partition", "目标表": "Hive", "分区字段": "dt",
         "分区粒度": "day", "保留天数": "90"},
        {"id": 2, "status": "已启用", "pending": False, "abnormal": False,
         "配置名称": "dwd_event_hourly_partition", "目标表": "Iceberg", "分区字段": "event_hour",
         "分区粒度": "hour", "保留天数": "30"},
        {"id": 3, "status": "草稿", "pending": True, "abnormal": False,
         "配置名称": "ads_monthly_summary", "目标表": "ClickHouse", "分区字段": "month",
         "分区粒度": "month", "保留天数": "365"},
        {"id": 4, "status": "已冻结", "pending": False, "abnormal": True,
         "配置名称": "deprecated_raw_log_partition", "目标表": "HDFS", "分区字段": "log_date",
         "分区粒度": "day", "保留天数": "7"},
    ],
    "data_quality_rule": [
        {"id": 1, "status": "已通过", "pending": False, "abnormal": False,
         "规则名称": "订单金额非负校验", "校验维度": "accuracy", "目标表": "ods_orders",
         "阈值": "0.99", "最近分数": "0.998"},
        {"id": 2, "status": "未通过", "pending": True, "abnormal": True,
         "规则名称": "用户事件完整性", "校验维度": "completeness", "目标表": "dwd_user_events",
         "阈值": "0.95", "最近分数": "0.872"},
        {"id": 3, "status": "待校验", "pending": True, "abnormal": False,
         "规则名称": "库存快照唯一性", "校验维度": "uniqueness", "目标表": "dwd_inventory_snapshot",
         "阈值": "1.0", "最近分数": ""},
        {"id": 4, "status": "已忽略", "pending": False, "abnormal": False,
         "规则名称": "旧日志格式校验", "校验维度": "consistency", "目标表": "legacy_raw_logs",
         "阈值": "0.80", "最近分数": "0.45"},
    ],
    "schema_change": [
        {"id": 1, "status": "待审批", "pending": True, "abnormal": False,
         "目标表": "ods_orders", "变更类型": "ADD_COLUMN", "变更内容": "添加字段 promotion_code VARCHAR(64)",
         "影响下游表数": "3", "变更时间": "2026-11-15"},
        {"id": 2, "status": "已执行", "pending": False, "abnormal": False,
         "目标表": "dwd_user_events", "变更类型": "MODIFY_TYPE", "变更内容": "event_data MAP<STRING,STRING>改为STRING",
         "影响下游表数": "5", "变更时间": "2026-10-20"},
        {"id": 3, "status": "已回退", "pending": False, "abnormal": True,
         "目标表": "dws_sales_agg", "变更类型": "DROP_COLUMN", "变更内容": "误删字段 customer_segment",
         "影响下游表数": "8", "变更时间": "2026-09-01"},
    ],
    "lineage_graph": [
        {"id": 1, "status": "已建立", "pending": False, "abnormal": False,
         "目标表": "dwd_orders_clean", "来源表列表": "ods_orders, dim_customer",
         "转换逻辑": "清洗+维度关联", "中间层级": "DWD", "血缘深度": "2"},
        {"id": 2, "status": "已建立", "pending": False, "abnormal": False,
         "目标表": "dws_sales_daily", "来源表列表": "dwd_orders_clean, dim_product",
         "转换逻辑": "按日聚合销售金额和订单数", "中间层级": "DWS", "血缘深度": "3"},
        {"id": 3, "status": "待验证", "pending": True, "abnormal": False,
         "目标表": "ads_executive_dashboard", "来源表列表": "dws_sales_daily, dim_region",
         "转换逻辑": "管理层看板聚合+同比计算", "中间层级": "ADS", "血缘深度": "4"},
        {"id": 4, "status": "已断裂", "pending": False, "abnormal": True,
         "目标表": "report_monthly_finance", "来源表列表": "legacy_gl_journal, dim_fiscal_period",
         "转换逻辑": "财务月报汇总", "中间层级": "报表", "血缘深度": "2"},
    ],
    "backfill_task": [
        {"id": 1, "status": "运行中", "pending": True, "abnormal": False,
         "目标表": "dws_sales_daily", "补数时间范围": "2026-11-01~2026-11-20",
         "补数状态": "回填中", "已完成分区": "15", "预计行数": "800000"},
        {"id": 2, "status": "排队中", "pending": True, "abnormal": False,
         "目标表": "ods_orders", "补数时间范围": "2026-10-15~2026-10-31",
         "补数状态": "排队中", "已完成分区": "0", "预计行数": "450000"},
        {"id": 3, "status": "已完成", "pending": False, "abnormal": False,
         "目标表": "dwd_user_events", "补数时间范围": "2026-11-10~2026-11-15",
         "补数状态": "完成", "已完成分区": "6", "预计行数": "320000"},
        {"id": 4, "status": "补数失败", "pending": True, "abnormal": True,
         "目标表": "ads_legacy_report", "补数时间范围": "2026-01-01~2026-06-30",
         "补数状态": "失败-内存溢出", "已完成分区": "23", "预计行数": "5200000"},
    ],
    "table_archive": [
        {"id": 1, "status": "已启用", "pending": False, "abnormal": False,
         "策略名称": "ods_orders_history_archive", "目标表": "ods_orders",
         "归档条件": "分区日期早于 180 天", "归档存储": "S3_IA", "原始保留天数": "365"},
        {"id": 2, "status": "草稿", "pending": True, "abnormal": False,
         "策略名称": "legacy_logs_cold", "目标表": "legacy_raw_logs",
         "归档条件": "分区日期早于 90 天", "归档存储": "S3_Glacier", "原始保留天数": "2555"},
        {"id": 3, "status": "已停用", "pending": False, "abnormal": True,
         "策略名称": "deprecated_user_clicks_archive", "目标表": "user_clicks_2024",
         "归档条件": "所有分区", "归档存储": "S3_IA", "原始保留天数": "30"},
    ],
    "agg_builder": [
        {"id": 1, "status": "已启用", "pending": False, "abnormal": False,
         "配置名称": "sales_by_region_daily", "源事实表": "dwd_orders_clean",
         "聚合维度": "region, dt", "聚合函数": "SUM", "更新频率": "daily"},
        {"id": 2, "status": "已启用", "pending": False, "abnormal": False,
         "配置名称": "user_behavior_count_hourly", "源事实表": "dwd_user_events",
         "聚合维度": "event_type, event_hour", "聚合函数": "COUNT", "更新频率": "hourly"},
        {"id": 3, "status": "草稿", "pending": True, "abnormal": False,
         "配置名称": "product_avg_price_monthly", "源事实表": "dim_product",
         "聚合维度": "category, month", "聚合函数": "AVG", "更新频率": "weekly"},
    ],
    "pipeline_monitor": [
        {"id": 1, "status": "成功", "pending": False, "abnormal": False,
         "流水线名称": "ods_order_inc_daily", "运行时间": "2026-11-21T02:05:00Z",
         "执行状态": "完成", "处理行数": "150000", "错误信息": ""},
        {"id": 2, "status": "部分成功", "pending": True, "abnormal": True,
         "流水线名称": "dwd_user_event_cdc", "运行时间": "2026-11-21T07:30:00Z",
         "执行状态": "3/4 阶段完成", "处理行数": "980000", "错误信息": "目标端写入阶段超时"},
        {"id": 3, "status": "执行失败", "pending": True, "abnormal": True,
         "流水线名称": "legacy_mysql_dump_daily", "运行时间": "2026-11-21T01:15:00Z",
         "执行状态": "失败", "处理行数": "0", "错误信息": "连接池耗尽: too many connections"},
        {"id": 4, "status": "超时终止", "pending": True, "abnormal": True,
         "流水线名称": "ods_inventory_full_monthly", "运行时间": "2026-11-01T03:45:00Z",
         "执行状态": "超时", "处理行数": "1200000", "错误信息": "全量抽取超过600秒限制"},
    ],
    "cold_restore": [
        {"id": 1, "status": "排队中", "pending": True, "abnormal": False,
         "归档来源": "S3_IA/ods_orders_history", "恢复目标路径": "/data/hot_restore/ods_orders_2024",
         "恢复时间范围": "2024-10-01~2024-12-31", "恢复状态": "排队中", "预计文件大小": "12 GB"},
        {"id": 2, "status": "已完成", "pending": False, "abnormal": False,
         "归档来源": "S3_Glacier/audit_logs_2023", "恢复目标路径": "/data/hot_restore/audit_2023Q4",
         "恢复时间范围": "2023-10-01~2023-12-31", "恢复状态": "完成", "预计文件大小": "3.5 GB"},
        {"id": 3, "status": "恢复失败", "pending": True, "abnormal": True,
         "归档来源": "S3_IA/user_clicks_2024", "恢复目标路径": "/data/hot_restore/user_clicks",
         "恢复时间范围": "2024-01-01~2024-06-30", "恢复状态": "失败", "预计文件大小": "28 GB"},
    ],
    "dimension_builder": [
        {"id": 1, "status": "已上线", "pending": False, "abnormal": False,
         "配置名称": "dim_customer_scd2", "来源表": "customer_master",
         "维度键": "customer_id", "缓慢变化处理": "SCD_TYPE_2", "历史快照": "dim_customer_history"},
        {"id": 2, "status": "草稿", "pending": True, "abnormal": False,
         "配置名称": "dim_product_scd1", "来源表": "product_catalog",
         "维度键": "product_id", "缓慢变化处理": "SCD_TYPE_1", "历史快照": ""},
        {"id": 3, "status": "已停用", "pending": False, "abnormal": True,
         "配置名称": "dim_supplier_legacy", "来源表": "supplier_master",
         "维度键": "supplier_code", "缓慢变化处理": "SCD_TYPE_2", "历史快照": "dim_supplier_history"},
    ],
    "data_retention": [
        {"id": 1, "status": "已生效", "pending": False, "abnormal": False,
         "规则名称": "ods_orders_hot_tier_90d", "目标数据集": "ods_orders", "适用范围": "热层副本",
         "保留天数": "90", "清理方式": "降冷", "最近执行时间": "2026-11-20T02:00:00Z"},
        {"id": 2, "status": "已生效", "pending": False, "abnormal": False,
         "规则名称": "ods_orders_compliance_365d", "目标数据集": "ods_orders", "适用范围": "全量明细",
         "保留天数": "365", "清理方式": "仅标记过期", "最近执行时间": "2026-11-20T02:00:00Z"},
        {"id": 3, "status": "待生效", "pending": True, "abnormal": False,
         "规则名称": "dwd_events_30d_retention", "目标数据集": "dwd_user_events", "适用范围": "全量明细",
         "保留天数": "30", "清理方式": "降冷", "最近执行时间": ""},
        {"id": 4, "status": "已过期", "pending": False, "abnormal": True,
         "规则名称": "legacy_7d_force_delete", "目标数据集": "legacy_raw_logs", "适用范围": "全量明细",
         "保留天数": "7", "清理方式": "物理删除", "最近执行时间": "2026-11-14T02:00:00Z"},
    ],
    "transform_plan": [
        {"id": 1, "status": "已发布", "pending": False, "abnormal": False,
         "方案名称": "订单清洗与标准化", "步骤序列": "ods_orders->cleaned_orders(去重+类型转换),cleaned_orders->enriched_orders(维度关联JOIN)",
         "输入表": "ods_orders", "输出表": "enriched_orders", "依赖关系": "dim_customer,dim_product"},
        {"id": 2, "status": "草稿", "pending": True, "abnormal": False,
         "方案名称": "用户行为漏斗分析", "步骤序列": "dwd_user_events->sessionized_events(会话聚合),sessionized_events->funnel_metrics(漏斗计算)",
         "输入表": "dwd_user_events", "输出表": "funnel_metrics", "依赖关系": "dim_event_type"},
        {"id": 3, "status": "已停用", "pending": False, "abnormal": False,
         "方案名称": "旧的库存日报", "步骤序列": "inventory_raw->inventory_daily(日聚合)",
         "输入表": "inventory_raw", "输出表": "inventory_daily", "依赖关系": ""},
    ],
    "data_migration": [
        {"id": 1, "status": "待迁", "pending": True, "abnormal": False,
         "任务名称": "Hive到Iceberg订单迁移", "源存储": "Hive", "目标存储": "Iceberg",
         "迁移范围": "ods_orders,dwd_orders_clean", "校验方式": "行数+校验和+分区完整性"},
        {"id": 2, "status": "已校验", "pending": False, "abnormal": False,
         "任务名称": "MySQL到ClickHouse实时同步", "源存储": "MySQL", "目标存储": "ClickHouse",
         "迁移范围": "dws_sales_daily", "校验方式": "行数+校验和"},
        {"id": 3, "status": "迁移失败", "pending": True, "abnormal": True,
         "任务名称": "HDFS到S3冷数据搬迁", "源存储": "HDFS", "目标存储": "S3",
         "迁移范围": "legacy_raw_logs", "校验方式": "行数+分区完整性"},
    ],
    "delta_merge": [
        {"id": 1, "status": "已完成", "pending": False, "abnormal": False,
         "目标表": "ods_orders", "增量来源": "MySQL_binlog_orders",
         "合并键": "order_id", "冲突策略": "newest_wins", "合并状态": "完成"},
        {"id": 2, "status": "排队中", "pending": True, "abnormal": False,
         "目标表": "dwd_user_events", "增量来源": "Kafka_user_events_delta",
         "合并键": "event_id", "冲突策略": "max_timestamp", "合并状态": "排队中"},
        {"id": 3, "status": "合并失败", "pending": True, "abnormal": True,
         "目标表": "dws_sales_daily", "增量来源": "dwd_orders_late_events",
         "合并键": "order_date", "冲突策略": "custom_merge_func", "合并状态": "失败-主键冲突"},
    ],
    "snapshot_creator": [
        {"id": 1, "status": "已完成", "pending": False, "abnormal": False,
         "目标数据集": "ods_orders", "快照时间点": "2026-11-21T00:00:00Z",
         "快照类型": "daily_sealed", "存储路径": "/snapshots/ods_orders/dt=2026-11-21", "文件大小": "4.2 GB"},
        {"id": 2, "status": "创建中", "pending": True, "abnormal": False,
         "目标数据集": "dws_sales_daily", "快照时间点": "2026-11-21T06:00:00Z",
         "快照类型": "incremental", "存储路径": "/snapshots/dws_sales_daily/inc_20261121", "文件大小": "0.8 GB"},
        {"id": 3, "status": "创建失败", "pending": True, "abnormal": True,
         "目标数据集": "legacy_raw_logs", "快照时间点": "2026-11-20T00:00:00Z",
         "快照类型": "full", "存储路径": "/snapshots/legacy_raw_logs/full_20261120", "文件大小": ""},
    ],
    "data_lake_audit": [
        {"id": 1, "status": "审计中", "pending": True, "abnormal": False,
         "审计范围": "全平台20个业务模块", "审计时间": "2026-11-21",
         "表总数": "20", "异常表数": "", "审计结论": ""},
        {"id": 2, "status": "已通过", "pending": False, "abnormal": False,
         "审计范围": "ODS层表", "审计时间": "2026-11-15",
         "表总数": "5", "异常表数": "0", "审计结论": "所有元数据完整"},
        {"id": 3, "status": "未通过", "pending": True, "abnormal": True,
         "审计范围": "报表层表", "审计时间": "2026-11-01",
         "表总数": "3", "异常表数": "2", "审计结论": "2张表存在孤儿文件和断裂血缘"},
    ],
    "sla_policy": [
        {"id": 1, "status": "已生效", "pending": False, "abnormal": False,
         "规则名称": "ods_orders_T+0_7AM", "关联流水线": "ods_order_inc_daily",
         "产出截止时间": "2026-11-21T07:00:00", "预警提前量": "30", "通知方式": "email,dingtalk"},
        {"id": 2, "status": "待生效", "pending": True, "abnormal": False,
         "规则名称": "dws_sales_T+0_9AM", "关联流水线": "dws_sales_agg_hourly",
         "产出截止时间": "2026-11-21T09:00:00", "预警提前量": "60", "通知方式": "wecom"},
        {"id": 3, "status": "已停用", "pending": False, "abnormal": True,
         "规则名称": "legacy_report_T+1_10AM", "关联流水线": "legacy_mysql_dump_daily",
         "产出截止时间": "2026-11-20T10:00:00", "预警提前量": "15", "通知方式": "email"},
    ],
    "cleanup_job": [
        {"id": 1, "status": "已启用", "pending": False, "abnormal": False,
         "作业名称": "cleanup_expired_ods_partitions", "目标路径": "/data/hot/ods_orders",
         "清理条件": "命中保留规则分区：超 90 天热层转冷，超 365 天物理删除", "预览行数": "4500000", "最近清理时间": "2026-11-20T03:00:00Z"},
        {"id": 2, "status": "已暂停", "pending": False, "abnormal": True,
         "作业名称": "cleanup_temp_merge_files", "目标路径": "/data/staging/_temporary",
         "清理条件": "文件超过24小时未修改", "预览行数": "0", "最近清理时间": "2026-11-15T02:00:00Z"},
        {"id": 3, "status": "草稿", "pending": True, "abnormal": False,
         "作业名称": "cleanup_orphan_snapshot_files", "目标路径": "/snapshots",
         "清理条件": "无对应元数据引用的快照文件", "预览行数": "1200000", "最近清理时间": ""},
    ],
    "catalog_sync": [
        {"id": 1, "status": "已同步", "pending": False, "abnormal": False,
         "任务名称": "Hive_to_Glue_sync", "源Catalog": "hive_metastore_prod",
         "目标Catalog": "aws_glue_catalog", "同步范围": "ods_*,dwd_*,dws_*",
         "同步状态": "完成"},
        {"id": 2, "status": "同步中", "pending": True, "abnormal": False,
         "任务名称": "Iceberg_to_Unity_sync", "源Catalog": "iceberg_catalog",
         "目标Catalog": "unity_catalog_main", "同步范围": "dwd_user_events,dws_sales_daily",
         "同步状态": "进行中"},
        {"id": 3, "status": "同步失败", "pending": True, "abnormal": True,
         "任务名称": "Hive_to_HMS_legacy_sync", "源Catalog": "hive_metastore_legacy",
         "目标Catalog": "hms_production", "同步范围": "全部",
         "同步状态": "失败-Schema冲突2处"},
    ],
    # 内部台账（下划线前缀，不出现在模块概览）：订单分区物理状态与处置清单，
    # 仅供 data_retention / cleanup_job 两个领域服务共享读写。
    "_partition_inventory": _order_partition_inventory(),
    "_disposal_plan": [],
}