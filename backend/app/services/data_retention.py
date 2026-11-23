"""数据保留策略领域服务：管理数据保留规则、合规重叠检查和按策略触发清理。

数据链路位置：数据生命周期层，控制数据的保留期和自动清理。
判定口径：按业务基准日（2026-11-23）计算分区年龄；同表不同范围的
生效规则各自核对；分区处置结论取全部生效规则的综合结果，任一规则
仍在留存期内就不得物理删除。
"""

from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.lifecycle_common import (
    BUSINESS_TODAY,
    EXECUTION_TIME,
    EXPIRE_METHODS,
    HOT_ROOT,
    TIER_METHODS,
    effective_rules,
    inventory_partitions,
    partition_age,
    plan_summary,
    retention_days,
    rule_cutoff,
    rule_scope,
    safety_violations,
    scope_covers,
    scopes_overlap,
    sync_plan_table,
)
from app.store import store


class DataRetentionService(BaseModuleService):
    MODULE = "data_retention"
    ENTITY = "保留规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "目标数据集", "适用范围", "保留天数", "清理方式", "最近执行时间"]
    REQUIRED_FIELDS = ["规则名称", "目标数据集", "保留天数"]
    STATUS_ORDER = ["待生效", "已生效", "已过期"]
    ACTION_RULES = {"启用规则": "已生效", "停用规则": "已过期"}
    NEGATIVE_ACTIONS = ["停用规则"]

    CLEANUP_METHODS = frozenset({"物理删除", "移至归档存储", "降冷", "仅标记过期"})

    def evaluate_retention_status(self, entry_id: int) -> dict[str, Any]:
        """评估单条保留规则：按规则自身范围核对覆盖分区，列出实际到期的分区。

        输出该规则视角下的截止日期和到期分区清单（含每个分区的年龄、
        存储层和当前处置状态）；分区最终处置以多规则综合的处置清单为准。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "保留规则不存在"}

        dataset = entry.get("目标数据集", "")
        days = retention_days(entry)
        method = entry.get("清理方式", "移至归档存储")

        covered = [p for p in inventory_partitions(store, dataset) if scope_covers(entry, p)]
        expired = [p for p in covered if partition_age(p) > days]
        pending = [p for p in expired if p.get("处置状态") == "未处置"]
        size_mb = sum(int(p.get("大小MB", 0)) for p in expired)

        return {
            "policy_id": entry_id,
            "policy_name": entry.get("规则名称"),
            "dataset": dataset,
            "scope": rule_scope(entry),
            "business_date": BUSINESS_TODAY.isoformat(),
            "retention_days": days,
            "cutoff_date": rule_cutoff(entry).isoformat(),
            "cleanup_method": method,
            "covered_partitions": len(covered),
            "expired_partitions": len(expired),
            "pending_partitions": len(pending),
            "partitions": [
                {
                    "dataset": dataset,
                    "partition": f"dt={p.get('分区日期')}",
                    "age_days": partition_age(p),
                    "storage_tier": p.get("存储层"),
                    "action": method,
                    "disposal_state": p.get("处置状态"),
                    "replica_location": p.get("副本位置"),
                }
                for p in expired
            ],
            "estimated_cleanup_size_gb": round(size_mb / 1024, 2),
            "needs_action": len(pending) > 0,
            "note": "单规则视角的到期清单；分区最终处置以多规则综合的处置清单为准",
        }

    def check_compliance_overlap(self, entry_id: int) -> dict[str, Any]:
        """检查同表生效规则的重叠：每条规则各自核对，再判定是否存在破坏性冲突。

        只有「某规则会在另一规则留存期内销毁数据」才算冲突；
        转冷/归档类规则与合规保留规则分层并存，不算冲突。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "保留规则不存在"}

        dataset = entry.get("目标数据集", "")
        rules = effective_rules(store, dataset)
        partitions = inventory_partitions(store, dataset)

        rule_checks = []
        for rule in rules:
            days = retention_days(rule)
            covered = [p for p in partitions if scope_covers(rule, p)]
            expired = [p for p in covered if partition_age(p) > days]
            rule_checks.append({
                "rule_id": rule.get("id"),
                "rule_name": rule.get("规则名称"),
                "scope": rule_scope(rule),
                "retention_days": days,
                "cutoff_date": rule_cutoff(rule).isoformat(),
                "cleanup_method": rule.get("清理方式"),
                "covered_partitions": len(covered),
                "expired_partitions": len(expired),
            })

        conflicts = []
        for rule_a in rules:
            if rule_a.get("清理方式") not in EXPIRE_METHODS:
                continue
            for rule_b in rules:
                if rule_b is rule_a or retention_days(rule_b) <= retention_days(rule_a):
                    continue
                if not scopes_overlap(rule_a, rule_b):
                    continue
                conflicts.append({
                    "other_policy_id": rule_b.get("id"),
                    "other_policy_name": rule_b.get("规则名称"),
                    "other_retention_days": retention_days(rule_b),
                    "conflict": (
                        f"「{rule_a.get('规则名称')}」{retention_days(rule_a)} 天即"
                        f"{rule_a.get('清理方式')}，但「{rule_b.get('规则名称')}」"
                        f"要求保留 {retention_days(rule_b)} 天"
                    ),
                    "resolution": "以较长保留期为准，物理删除须等待全部生效规则留存期届满",
                })

        effective = max((retention_days(r) for r in rules), default=0)
        overlapping = [r for r in rules if r.get("id") != entry_id]
        tier_days = min(
            (retention_days(r) for r in rules if r.get("清理方式") in TIER_METHODS),
            default=0,
        )
        return {
            "policy_id": entry_id,
            "dataset": dataset,
            "business_date": BUSINESS_TODAY.isoformat(),
            "overlapping_policies": len(overlapping),
            "rule_checks": rule_checks,
            "conflicts": conflicts,
            "effective_retention_days": effective,
            "layered_policy": (
                f"热层副本 {tier_days} 天转冷，全量明细 {effective} 天内保持可回查，"
                f"留存期届满才物理删除"
            ) if rules else "无生效规则",
            "recommendation": "存在破坏性冲突，需调整规则" if conflicts else "无破坏性冲突，规则分层生效",
        }

    def build_disposal_plan(self, dataset: str = "ods_orders") -> dict[str, Any]:
        """按实际生效规则和业务基准日，计算订单分区逐个的处置清单。

        每个分区给出处置结论和依据；多个截止日期并存时逐条列明命中规则。
        已确认的转冷/删除结果从分区台账继承，重复生成不会推翻已完成的处置。
        """
        rules = effective_rules(store, dataset)
        if not rules:
            return {"ok": False, "message": f"数据集 {dataset} 没有已生效的保留规则"}
        rows = sync_plan_table(store, dataset)
        summary = plan_summary(rows)
        violations = safety_violations(rows)
        return {
            "ok": True,
            "dataset": dataset,
            "business_date": BUSINESS_TODAY.isoformat(),
            "rules": [
                {
                    "rule_id": rule.get("id"),
                    "rule_name": rule.get("规则名称"),
                    "scope": rule_scope(rule),
                    "retention_days": retention_days(rule),
                    "cutoff_date": rule_cutoff(rule).isoformat(),
                    "cleanup_method": rule.get("清理方式"),
                }
                for rule in rules
            ],
            "cutoff_dates": {rule.get("规则名称"): rule_cutoff(rule).isoformat() for rule in rules},
            **summary,
            "safety_check": {
                "ok": not violations,
                "invariant": "留存期内分区完整可回查，不进入物理删除清单",
                "violations": violations,
            },
            "partitions": rows,
        }

    def verify_in_retention_queryable(self, dataset: str = "ods_orders") -> dict[str, Any]:
        """校验留存期内分区完整可回查，且未出现在物理删除清单。"""
        rules = effective_rules(store, dataset)
        if not rules:
            return {"ok": False, "message": f"数据集 {dataset} 没有已生效的保留规则"}
        rows = sync_plan_table(store, dataset)
        violations = safety_violations(rows)
        in_retention = [r for r in rows if r["年龄天数"] <= r["最长保留天数"]]
        queryable = [r for r in in_retention if r.get("副本位置")]
        return {
            "ok": not violations,
            "dataset": dataset,
            "business_date": BUSINESS_TODAY.isoformat(),
            "in_retention_partitions": len(in_retention),
            "queryable_partitions": len(queryable),
            "violations": violations,
            "message": (
                "留存期内分区均完整可回查，未进入物理删除清单"
                if not violations else "发现留存期安全违规，需立即处理"
            ),
        }

    def trigger_cleanup_by_policy(self, entry_id: int) -> dict[str, Any]:
        """根据保留策略触发清理作业：生成处置清单并联动清理作业。

        同一数据集已有启用中的清理作业时复用，不重复创建；
        已完成的迁移/删除由处置清单继承，不会重复执行。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "保留规则不存在"}
        if entry.get("status") != "已生效":
            return {"ok": False, "message": "只有已生效的规则才能触发清理"}

        dataset = entry.get("目标数据集", "")
        plan = self.build_disposal_plan(dataset)
        if not plan.get("ok"):
            return plan
        evaluation = self.evaluate_retention_status(entry_id)
        entry["最近执行时间"] = EXECUTION_TIME

        pending_total = plan["status_counts"].get("待执行", 0)
        if pending_total == 0:
            return {
                "ok": True,
                "policy_id": entry_id,
                "dataset": dataset,
                "cleanup_method": evaluation["cleanup_method"],
                "expired_partitions": evaluation["expired_partitions"],
                "status_counts": plan["status_counts"],
                "message": "无需清理：分区均在留存期内或已完成处置，不重复执行",
            }

        condition_parts = []
        if plan["pending_cold_range"]:
            rng = plan["pending_cold_range"]
            condition_parts.append(f"转冷 {rng['start']}~{rng['end']}")
        if plan["pending_delete_range"]:
            rng = plan["pending_delete_range"]
            condition_parts.append(f"物理删除 {rng['start']}~{rng['end']}")
        condition = "；".join(condition_parts)

        jobs = store.rows("cleanup_job")
        existing = next(
            (j for j in jobs
             if str(j.get("目标路径", "")).rstrip("/").endswith(f"/{dataset}")
             and j.get("status") == "已启用"),
            None,
        )
        created = False
        if existing is None:
            new_id = max((int(j.get("id", 0)) for j in jobs), default=0) + 1
            existing = {
                "id": new_id,
                "status": "已启用",
                "作业名称": f"auto_cleanup_{dataset}",
                "目标路径": f"{HOT_ROOT}/{dataset}",
                "清理条件": condition,
                "预览行数": "0",
                "最近清理时间": "",
                "pending": False,
                "abnormal": False,
            }
            jobs.append(existing)
            created = True
        else:
            existing["清理条件"] = condition

        return {
            "ok": True,
            "policy_id": entry_id,
            "dataset": dataset,
            "cleanup_method": evaluation["cleanup_method"],
            "expired_partitions": evaluation["expired_partitions"],
            "created_cleanup_job_id": existing["id"] if created else None,
            "reused_cleanup_job_id": None if created else existing["id"],
            "cleanup_condition": condition,
            "pending_cold_range": plan["pending_cold_range"],
            "pending_delete_range": plan["pending_delete_range"],
            "message": (
                f"已创建清理作业 {existing['id']}" if created
                else f"复用已启用清理作业 {existing['id']}，未重复创建"
            ),
        }

    def retention_policy_report(self) -> dict[str, Any]:
        """生成保留策略全平台汇总报告：规则状态 + 各数据集实际处置进度。"""
        all_rules = store.rows(self.MODULE)
        active = [r for r in all_rules if r.get("status") == "已生效"]
        expiring_soon: list[dict[str, Any]] = []
        for r in active:
            retention = retention_days(r)
            if retention <= 7:
                expiring_soon.append({
                    "policy_id": r["id"],
                    "policy_name": r.get("规则名称"),
                    "dataset": r.get("目标数据集"),
                    "retention_days": retention,
                    "cleanup_method": r.get("清理方式"),
                })

        disposal_overview = []
        for dataset in sorted({r.get("目标数据集") for r in active}):
            if not inventory_partitions(store, dataset):
                continue
            rows = sync_plan_table(store, dataset)
            summary = plan_summary(rows)
            disposal_overview.append({
                "dataset": dataset,
                "status_counts": summary["status_counts"],
                "pending_cold_range": summary["pending_cold_range"],
                "pending_delete_range": summary["pending_delete_range"],
            })

        return {
            "total_policies": len(all_rules),
            "active_policies": len(active),
            "total_datasets_managed": len({r.get("目标数据集") for r in active}),
            "expiring_soon_count": len(expiring_soon),
            "expiring_soon": expiring_soon,
            "avg_retention_days": round(
                sum(retention_days(r) for r in active) / max(len(active), 1), 1
            ),
            "disposal_overview": disposal_overview,
            "next_scheduled_cleanup": "2026-11-24T02:00:00Z",
        }
