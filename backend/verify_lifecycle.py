"""端到端验证：数据保留冲突判定 + 清理执行稳定性。

场景对应任务要求：
1. 同表两条不同范围的生效规则各自核对（90 天热层转冷 + 365 天合规留存）；
2. 保存 120 天的订单分区命中两条规则 → 结论转冷、不得物理删除，依据写清；
3. 清理作业按清单生成本次能执行的日期范围，执行后回写副本位置；
4. 重复提交清理请求 → 已完成的迁移/删除不重复执行；
5. 值班员按清单记录的存储位置回查 120 天分区明细；
6. 保存 400 天的分区对照：核对实际可删范围、执行后状态、再次生成的清单；
7. 留存期内分区完整可回查，不出现在物理删除清单。

运行：cd backend && .venv/bin/python verify_lifecycle.py
"""

from __future__ import annotations

from app.services.cleanup_job import CleanupJobService
from app.services.data_retention import DataRetentionService

retention = DataRetentionService()
cleanup = CleanupJobService()

DATASET = "ods_orders"
DAY_120 = "2026-07-26"   # 保存 120 天的订单分区
DAY_400 = "2025-10-19"   # 保存 400 天的对照分区

checks: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    checks.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


# ── 1. 冲突判定：两条规则各自核对 ─────────────────────────────
overlap = retention.check_compliance_overlap(1)
check("同表两条生效规则各自核对", len(overlap["rule_checks"]) == 2,
      f"rule_checks={[(r['rule_name'], r['scope'], r['cutoff_date']) for r in overlap['rule_checks']]}")
check("转冷+合规分层并存不算破坏性冲突", overlap["conflicts"] == [],
      f"conflicts={overlap['conflicts']}")
check("有效保留期取最长 365 天", overlap["effective_retention_days"] == 365)

# ── 2. 生成处置清单：120 天 vs 400 天 ─────────────────────────
plan = retention.build_disposal_plan(DATASET)
row_120 = next(r for r in plan["partitions"] if r["分区日期"] == DAY_120)
row_400 = next(r for r in plan["partitions"] if r["分区日期"] == DAY_400)
check("清单按实际规则和日期生成", plan["ok"] and plan["total_partitions"] == 418,
      f"共 {plan['total_partitions']} 个分区，截止日期 {plan['cutoff_dates']}")
check("120 天分区结论=转冷", row_120["处置结论"] == "转冷" and row_120["状态"] == "待执行",
      f"年龄 {row_120['年龄天数']} 天")
check("120 天分区依据写清两条规则", "转冷" in row_120["处置依据"] and "不得物理删除" in row_120["处置依据"],
      row_120["处置依据"])
check("400 天分区结论=物理删除", row_400["处置结论"] == "物理删除" and row_400["状态"] == "待执行",
      f"年龄 {row_400['年龄天数']} 天，依据：{row_400['处置依据']}")
check("实际可删范围只含超 365 天的分区",
      plan["pending_delete_range"] == {"start": "2025-10-01", "end": "2025-11-22", "count": 52}
      and DAY_120 not in plan["deletable_partitions"] and DAY_400 in plan["deletable_partitions"],
      f"可删 {plan['pending_delete_range']}")
check("本次可转冷范围", plan["pending_cold_range"] == {"start": "2025-11-23", "end": "2026-08-24", "count": 273},
      f"{plan['pending_cold_range']}")
check("留存期安全校验通过", plan["safety_check"]["ok"], f"{plan['safety_check']['violations']}")

# ── 3. 触发清理：复用已启用作业，不重复创建 ────────────────────
trigger1 = retention.trigger_cleanup_by_policy(1)
trigger2 = retention.trigger_cleanup_by_policy(1)
check("重复触发复用同一清理作业",
      trigger1["reused_cleanup_job_id"] == 1 and trigger2["reused_cleanup_job_id"] == 1,
      f"执行条件：{trigger1['cleanup_condition']}")

# ── 4. 预览：本次能执行的日期范围 ─────────────────────────────
preview = cleanup.preview_cleanup_scope(1)
check("预览给出本次能执行的日期范围",
      preview["executable_ranges"]["转冷"]["count"] == 273
      and preview["executable_ranges"]["物理删除"]["count"] == 52,
      f"{preview['executable_ranges']}，已完成 {preview['already_done']}")
cand_120 = next(c for c in preview["candidates"] if c["partition"] == DAY_120)
check("120 天分区在预览中带依据和目标位置",
      cand_120["action"] == "转冷" and cand_120["target_location"].startswith("s3://lake-cold/"))

# ── 5. 执行：回写副本位置；重复执行不重复 ─────────────────────
first = cleanup.execute_cleanup(1, confirm=True)
check("首次执行完成转冷+删除",
      first["executed_cold_count"] == 273 and first["executed_delete_count"] == 52,
      f"转冷 {first['executed_cold_range']}，删除 {first['executed_delete_range']}，"
      f"释放热层 {first['freed_hot_gb']} GB")
check("预置的已完成分区被跳过", first["skipped_already_done"] == 3)
item_120 = next(i for i in first["executed_items"] if i["partition"] == DAY_120)
check("120 天分区副本位置已回写清单",
      item_120["replica_location"] == "s3://lake-cold/ods_orders/dt=2026-07-26")

second = cleanup.execute_cleanup(1, confirm=True)
check("重复提交不再执行已完成操作",
      second["executed_cold_count"] == 0 and second["executed_delete_count"] == 0
      and second["skipped_already_done"] == 328,
      second["message"])

# ── 6. 值班员按清单位置回查 120 天分区明细 ─────────────────────
recall_120 = cleanup.recall_partition(DATASET, DAY_120)
check("120 天分区转冷后仍可回查",
      recall_120["ok"] and recall_120["queryable"]
      and recall_120["replica_location"] == "s3://lake-cold/ods_orders/dt=2026-07-26"
      and recall_120["storage_tier"] == "冷层" and recall_120["within_retention"],
      f"位置 {recall_120['replica_location']}，行数 {recall_120['row_count']}，"
      f"样例 {recall_120['sample_rows'][0]['order_id']} 等 {len(recall_120['sample_rows'])} 条")

recall_400 = cleanup.recall_partition(DATASET, DAY_400)
check("400 天分区已删除不可回查", not recall_400["queryable"], recall_400["message"])

# ── 7. 再次生成清单：不重复已确认操作，留存期安全 ──────────────
plan2 = retention.build_disposal_plan(DATASET)
row_120_v2 = next(r for r in plan2["partitions"] if r["分区日期"] == DAY_120)
row_400_v2 = next(r for r in plan2["partitions"] if r["分区日期"] == DAY_400)
check("再次生成的清单无待执行项",
      plan2["status_counts"].get("待执行", 0) == 0
      and plan2["status_counts"]["已转冷"] == 275 and plan2["status_counts"]["已删除"] == 53)
check("120 天分区在清单中保持已转冷+冷层位置",
      row_120_v2["状态"] == "已转冷"
      and row_120_v2["副本位置"] == "s3://lake-cold/ods_orders/dt=2026-07-26")
check("400 天分区在清单中保持已删除", row_400_v2["状态"] == "已删除")

safety = retention.verify_in_retention_queryable(DATASET)
check("留存期内 365 个分区全部完整可回查",
      safety["ok"] and safety["in_retention_partitions"] == 365
      and safety["queryable_partitions"] == 365,
      safety["message"])

trigger3 = retention.trigger_cleanup_by_policy(1)
check("清单清空后触发不重复执行", "无需清理" in trigger3["message"], trigger3["message"])

# ── 汇总 ────────────────────────────────────────────────────
failed = [name for name, ok, _ in checks if not ok]
print(f"\n共 {len(checks)} 项检查，失败 {len(failed)} 项")
if failed:
    raise SystemExit(f"未通过：{failed}")
print("全部通过")
