"""清理作业领域服务：按处置清单执行过期分区的转冷与物理删除。

数据链路位置：存储治理层，清理过期数据和孤儿文件释放存储空间。
执行口径：
- 按数据保留策略生成的处置清单确定本次能执行的日期范围；
- 执行后把每个分区的副本位置回写到处置清单和分区台账；
- 已完成的迁移/删除不重复执行，重复提交只得到已完成的回执；
- 留存期内的分区绝不进入物理删除范围。
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.services.base import BaseModuleService
from app.services.lifecycle_common import (
    BUSINESS_TODAY,
    CONCLUSION_DELETE,
    CONCLUSION_TO_COLD,
    DONE_STATES,
    EXECUTION_TIME,
    INVENTORY_TABLE,
    PLAN_TABLE,
    cold_location,
    effective_rules,
    inventory_partitions,
    partition_age,
    plan_summary,
    retention_days,
    rule_cutoff,
    rule_scope,
    scope_covers,
    sync_plan_table,
)
from app.store import store


class CleanupJobService(BaseModuleService):
    MODULE = "cleanup_job"
    ENTITY = "清理作业"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["作业名称", "目标路径", "清理条件", "预览行数", "最近清理时间"]
    REQUIRED_FIELDS = ["作业名称", "目标路径", "清理条件"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用清理": "已启用", "暂停清理": "已暂停"}
    NEGATIVE_ACTIONS = []

    def _dataset_of(self, entry: dict[str, Any]) -> str:
        path = str(entry.get("目标路径", "")).rstrip("/")
        return path.split("/")[-1] if path else ""

    def _ensure_plan(self, dataset: str) -> list[dict[str, Any]] | None:
        if not effective_rules(store, dataset):
            return None
        return sync_plan_table(store, dataset)

    def preview_cleanup_scope(self, entry_id: int) -> dict[str, Any]:
        """预览清理范围：按处置清单列出本次能执行的分区、日期范围和依据。

        不再只给截止日期——每个分区都带处置结论、目标位置和判定依据，
        值班员可以据此判断某一天的数据能否转冷或删除。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "清理作业不存在"}

        dataset = self._dataset_of(entry)
        rows = self._ensure_plan(dataset)
        if rows is None:
            return {
                "ok": False,
                "job_id": entry_id,
                "job_name": entry.get("作业名称"),
                "target_path": entry.get("目标路径"),
                "message": "目标路径未关联已生效保留规则，无法生成处置清单",
            }

        inventory = {
            (p.get("数据集"), p.get("分区日期")): p
            for p in inventory_partitions(store, dataset)
        }
        candidates = []
        for row in rows:
            if row["状态"] != "待执行":
                continue
            part = inventory.get((dataset, row["分区日期"]), {})
            candidates.append({
                "path": row["副本位置"],
                "partition": row["分区日期"],
                "action": row["处置结论"],
                "row_count": int(part.get("行数", 0)),
                "size_mb": int(part.get("大小MB", 0)),
                "age_days": row["年龄天数"],
                "target_location": (
                    cold_location(dataset, row["分区日期"])
                    if row["处置结论"] == CONCLUSION_TO_COLD else "（物理删除）"
                ),
                "basis": row["处置依据"],
            })

        total_mb = sum(c["size_mb"] for c in candidates)
        entry["预览行数"] = str(sum(c["row_count"] for c in candidates))
        summary = plan_summary(rows)

        return {
            "ok": True,
            "job_id": entry_id,
            "job_name": entry.get("作业名称"),
            "target_path": entry.get("目标路径"),
            "dataset": dataset,
            "condition": entry.get("清理条件"),
            "business_date": BUSINESS_TODAY.isoformat(),
            "matched_items": len(candidates),
            "total_rows_to_migrate": sum(c["row_count"] for c in candidates if c["action"] == CONCLUSION_TO_COLD),
            "total_rows_to_delete": sum(c["row_count"] for c in candidates if c["action"] == CONCLUSION_DELETE),
            "total_size_mb": total_mb,
            "total_size_gb": round(total_mb / 1024, 2),
            "executable_ranges": {
                "转冷": summary["pending_cold_range"],
                "物理删除": summary["pending_delete_range"],
            },
            "already_done": summary["done_counts"],
            "candidates": candidates,
        }

    def execute_cleanup(self, entry_id: int, confirm: bool = False) -> dict[str, Any]:
        """执行清理作业：按处置清单逐项处置，并把副本位置回写到清单。

        confirm=True 时才实际执行。执行前逐条复核留存期安全边界；
        已完成的迁移/删除跳过不重复执行，重复提交返回相同回执。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "清理作业不存在"}
        if not confirm:
            preview = self.preview_cleanup_scope(entry_id)
            preview["awaiting_confirmation"] = True
            return preview
        if entry.get("status") != "已启用":
            return {"ok": False, "message": "只有已启用的清理作业才能执行"}

        dataset = self._dataset_of(entry)
        rows = self._ensure_plan(dataset)
        if rows is None:
            return {"ok": False, "message": "目标数据集没有已生效保留规则，不执行任何处置"}

        inventory = {
            (p.get("数据集"), p.get("分区日期")): p
            for p in inventory_partitions(store, dataset)
        }
        job_name = str(entry.get("作业名称") or f"job_{entry_id}")
        executed_cold: list[dict[str, Any]] = []
        executed_delete: list[dict[str, Any]] = []
        skipped_done: list[dict[str, Any]] = []
        guarded: list[dict[str, Any]] = []
        freed_mb = 0
        deleted_rows = 0

        for row in rows:
            if row["状态"] in DONE_STATES:
                skipped_done.append(row)
                continue
            if row["状态"] != "待执行":
                continue
            # 稳定性兜底：留存期内分区绝不物理删除
            if row["处置结论"] == CONCLUSION_DELETE and row["年龄天数"] <= row["最长保留天数"]:
                guarded.append(row)
                continue
            part = inventory.get((dataset, row["分区日期"]))
            if part is None:
                continue
            if row["处置结论"] == CONCLUSION_TO_COLD:
                new_location = cold_location(dataset, row["分区日期"])
                part.update({
                    "存储层": "冷层", "副本位置": new_location, "处置状态": "已转冷",
                    "处置时间": EXECUTION_TIME, "处置作业": job_name,
                })
                row.update({
                    "状态": "已转冷", "存储层": "冷层", "副本位置": new_location,
                    "执行时间": EXECUTION_TIME, "执行作业": job_name,
                })
                freed_mb += int(part.get("大小MB", 0))
                executed_cold.append(row)
            elif row["处置结论"] == CONCLUSION_DELETE:
                part.update({
                    "存储层": "已删除", "副本位置": "", "处置状态": "已删除",
                    "处置时间": EXECUTION_TIME, "处置作业": job_name,
                })
                row.update({
                    "状态": "已删除", "存储层": "已删除", "副本位置": "",
                    "执行时间": EXECUTION_TIME, "执行作业": job_name,
                })
                freed_mb += int(part.get("大小MB", 0))
                deleted_rows += int(part.get("行数", 0))
                executed_delete.append(row)

        remaining = [r for r in rows if r["状态"] == "待执行"]
        entry["预览行数"] = str(sum(
            int(inventory.get((dataset, r["分区日期"]), {}).get("行数", 0))
            for r in remaining
        ))
        entry["最近清理时间"] = EXECUTION_TIME

        executed_items = [
            {
                "partition": r["分区日期"],
                "action": r["处置结论"],
                "replica_location": r["副本位置"],
                "executed_at": r["执行时间"],
            }
            for r in executed_cold + executed_delete
        ]
        if executed_items:
            message = (
                f"本次转冷 {len(executed_cold)} 个分区、物理删除 {len(executed_delete)} 个分区，"
                f"副本位置已回写处置清单；跳过已完成 {len(skipped_done)} 个（不重复执行）"
            )
        else:
            message = f"没有待执行项：已完成的迁移/删除不重复执行（跳过 {len(skipped_done)} 个）"

        return {
            "ok": True,
            "job_id": entry_id,
            "dataset": dataset,
            "executed_cold_count": len(executed_cold),
            "executed_delete_count": len(executed_delete),
            "executed_cold_range": _date_range(executed_cold),
            "executed_delete_range": _date_range(executed_delete),
            "deleted_partitions": len(executed_delete),
            "deleted_rows": deleted_rows,
            "skipped_already_done": len(skipped_done),
            "guarded_violations": len(guarded),
            "freed_hot_gb": round(freed_mb / 1024, 2),
            "freed_space_gb": round(freed_mb / 1024, 2),
            "cleanup_time": EXECUTION_TIME,
            "executed_items": executed_items,
            "message": message,
        }

    def recall_partition(self, dataset: str, partition_date: str) -> dict[str, Any]:
        """按处置清单回查分区明细：用清单记录的副本位置定位数据。

        留存期内的分区（含已转冷）必须能按清单位置回查明细；
        已物理删除的分区返回删除回执，说明删除时间和依据。
        """
        rows = self._ensure_plan(dataset)
        if rows is None:
            return {"ok": False, "message": f"数据集 {dataset} 没有已生效保留规则"}
        row = next((r for r in rows if r["分区日期"] == partition_date), None)
        if row is None:
            return {"ok": False, "message": f"分区 dt={partition_date} 不在处置清单内"}
        part = next(
            (p for p in inventory_partitions(store, dataset) if p["分区日期"] == partition_date),
            None,
        )

        if row["状态"] == "已删除":
            return {
                "ok": False,
                "queryable": False,
                "dataset": dataset,
                "partition": partition_date,
                "age_days": row["年龄天数"],
                "executed_at": row["执行时间"],
                "message": (
                    f"分区已于 {row['执行时间']} 物理删除（已保存 {row['年龄天数']} 天，"
                    f"超出 {row['最长保留天数']} 天留存期），无法回查"
                ),
            }

        location = row.get("副本位置") or (part or {}).get("副本位置", "")
        return {
            "ok": True,
            "queryable": True,
            "dataset": dataset,
            "partition": partition_date,
            "age_days": row["年龄天数"],
            "storage_tier": row["存储层"],
            "replica_location": location,
            "within_retention": row["年龄天数"] <= row["最长保留天数"],
            "row_count": int((part or {}).get("行数", 0)),
            "sample_rows": _sample_order_rows(partition_date),
            "message": f"分区可回查：按处置清单记录的副本位置 {location} 读取明细",
        }

    def sync_with_retention_policy(self, entry_id: int) -> dict[str, Any]:
        """与数据保留策略联动：不只给截止日期，直接给出逐分区的处置结论。

        值班员可以按分区日期逐天核对：能否转冷、能否删除、依据哪条规则。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "清理作业不存在"}

        dataset = self._dataset_of(entry)
        rules = effective_rules(store, dataset)
        suggestions = []
        for rule in rules:
            days = retention_days(rule)
            covered = [p for p in inventory_partitions(store, dataset) if scope_covers(rule, p)]
            expired = [p for p in covered if partition_age(p) > days]
            suggestions.append({
                "policy_id": rule.get("id"),
                "policy_name": rule.get("规则名称"),
                "scope": rule_scope(rule),
                "retention_days": days,
                "expired_before": rule_cutoff(rule).isoformat(),
                "covered_partitions": len(covered),
                "expired_partitions": len(expired),
                "action": f"到期分区按「{rule.get('清理方式')}」处置，最终结论以处置清单为准",
            })

        result: dict[str, Any] = {
            "job_id": entry_id,
            "dataset": dataset,
            "matched_retention_policies": len(rules),
            "suggestions": suggestions,
            "auto_update_suggestion": suggestions[0] if suggestions else None,
        }
        if rules:
            rows = sync_plan_table(store, dataset)
            result["disposal_plan_summary"] = plan_summary(rows)
            result["partition_decisions"] = [
                {
                    "partition": r["分区日期"],
                    "age_days": r["年龄天数"],
                    "conclusion": r["处置结论"],
                    "status": r["状态"],
                    "basis": r["处置依据"],
                    "replica_location": r["副本位置"],
                }
                for r in rows
            ]
        return result

    def detect_duplicate_cleanup_jobs(self) -> dict[str, Any]:
        """检测重复的清理作业。

        当多条清理作业针对相同目标路径时产生告警，
        避免重复清理和资源浪费。
        """
        all_jobs = store.rows(self.MODULE)
        path_groups: dict[str, list[dict[str, Any]]] = {}
        for job in all_jobs:
            path = job.get("目标路径", "")
            if path:
                path_groups.setdefault(path, []).append(job)

        duplicates = {k: v for k, v in path_groups.items() if len(v) > 1}
        return {
            "duplicate_groups": len(duplicates),
            "affected_jobs": sum(len(v) for v in duplicates.values()),
            "details": {
                path: [{"job_id": j["id"], "job_name": j.get("作业名称"), "status": j.get("status")} for j in jobs]
                for path, jobs in duplicates.items()
            },
        }

    def cleanup_summary_report(self, days: int = 7) -> dict[str, Any]:
        """生成清理作业的汇总报告：按处置清单和分区台账的实际状态统计。"""
        all_jobs = store.rows(self.MODULE)
        active = [j for j in all_jobs if j.get("status") == "已启用"]
        plan_rows = store.rows(PLAN_TABLE)
        inventory = store.rows(INVENTORY_TABLE)
        freed_mb = sum(
            int(p.get("大小MB", 0)) for p in inventory
            if p.get("处置状态") in ("已转冷", "已删除")
        )
        return {
            "period_days": days,
            "total_jobs": len(all_jobs),
            "active_jobs": len(active),
            "recent_cleanups": sum(1 for j in all_jobs if j.get("最近清理时间")),
            "total_freed_gb": round(freed_mb / 1024, 2),
            "migrated_partitions": sum(1 for r in plan_rows if r.get("状态") == "已转冷"),
            "cleaned_partitions": sum(1 for r in plan_rows if r.get("状态") == "已删除"),
            "pending_partitions": sum(1 for r in plan_rows if r.get("状态") == "待执行"),
            "cleaned_temp_files": 0,
            "next_scheduled_cleanup": "2026-11-24T02:00:00Z",
        }


def _date_range(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    days = sorted(str(row["分区日期"]) for row in rows)
    if not days:
        return None
    return {"start": days[0], "end": days[-1], "count": len(days)}


def _sample_order_rows(partition_date: str, count: int = 3) -> list[dict[str, Any]]:
    """生成确定性的订单明细样例，模拟按清单位置回查出的分区内容。"""
    try:
        day = date.fromisoformat(partition_date)
    except ValueError:
        return []
    base = day.toordinal()
    rows = []
    for i in range(count):
        rows.append({
            "order_id": f"OD{day.strftime('%Y%m%d')}{base % 1000:03d}{i:02d}",
            "user_id": f"U{10000 + (base * 7 + i * 13) % 89999}",
            "amount": round(((base * 13 + i * 971) % 500000) / 100, 2),
            "pay_status": ["已支付", "待支付", "已退款"][(base + i) % 3],
            "created_at": f"{partition_date}T{8 + i:02d}:15:00Z",
        })
    return rows
