"""数据生命周期共享逻辑：保留规则核对、分区处置结论与处置清单。

数据保留策略（data_retention）与清理作业（cleanup_job）共用同一份
订单分区台账和处置清单，保证「按实际规则和日期计算」的口径一致：

- 规则按各自适用范围核对：热层副本规则只核对热层分区，全量明细
  规则覆盖所有在册副本，同表不同范围的规则各自独立核对；
- 分区年龄以业务基准日 BUSINESS_TODAY 计算，避免各自取系统时钟
  导致清单结果漂移；
- 分区处置结论由全部生效规则综合决定：任一生效规则仍在留存期内，
  分区就必须完整可回查，不得进入物理删除清单；
- 已确认的转冷/删除结果写回分区台账，再次生成清单时继承台账状态，
  不会重复安排已完成的处置。
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

# 业务基准日：全平台统一按该日期计算分区年龄和规则截止日期。
BUSINESS_TODAY = date(2026, 11, 23)
EXECUTION_TIME = "2026-11-23T02:30:00Z"

INVENTORY_TABLE = "_partition_inventory"
PLAN_TABLE = "_disposal_plan"

HOT_ROOT = "/data/hot"
COLD_ROOT = "s3://lake-cold"

TIER_METHODS = frozenset({"降冷", "移至归档存储"})
EXPIRE_METHODS = frozenset({"物理删除", "仅标记过期"})

CONCLUSION_KEEP = "保留中"
CONCLUSION_TO_COLD = "转冷"
CONCLUSION_DELETE = "物理删除"

STATUS_PENDING = "待执行"
STATUS_COLD = "已转冷"
STATUS_DELETED = "已删除"
STATUS_KEEP = "保留中"
DONE_STATES = (STATUS_COLD, STATUS_DELETED)


def parse_day(text: Any) -> date:
    return date.fromisoformat(str(text).strip())


def partition_age(partition: dict[str, Any]) -> int:
    """分区年龄（天）：以业务基准日为准，保证清单结果可复算。"""
    return (BUSINESS_TODAY - parse_day(partition.get("分区日期", "1970-01-01"))).days


def retention_days(rule: dict[str, Any]) -> int:
    try:
        return int(str(rule.get("保留天数", "0")).strip() or "0")
    except (TypeError, ValueError):
        return 0


def rule_cutoff(rule: dict[str, Any]) -> date:
    """规则截止日期：分区日期早于该日期即命中该规则。"""
    return BUSINESS_TODAY - timedelta(days=retention_days(rule))


def rule_scope(rule: dict[str, Any]) -> str:
    return str(rule.get("适用范围") or "全量明细")


def scope_covers(rule: dict[str, Any], partition: dict[str, Any]) -> bool:
    """规则按各自适用范围核对分区。

    热层副本规则只核对热层分区，冷层副本规则只核对冷层分区，
    全量明细规则覆盖所有在册副本（已删除的不再核对）。
    """
    scope = rule_scope(rule)
    tier = str(partition.get("存储层", "热层"))
    if scope == "热层副本":
        return tier == "热层"
    if scope == "冷层副本":
        return tier == "冷层"
    return tier != "已删除"


def scopes_overlap(rule_a: dict[str, Any], rule_b: dict[str, Any]) -> bool:
    """两条规则的适用范围是否覆盖同一批分区。"""
    scope_a, scope_b = rule_scope(rule_a), rule_scope(rule_b)
    if scope_a == scope_b:
        return True
    return "全量明细" in (scope_a, scope_b)


def hot_location(dataset: str, day: str) -> str:
    return f"{HOT_ROOT}/{dataset}/dt={day}"


def cold_location(dataset: str, day: str) -> str:
    return f"{COLD_ROOT}/{dataset}/dt={day}"


def effective_rules(store: Any, dataset: str) -> list[dict[str, Any]]:
    """数据集上已生效的保留规则，按保留天数升序。"""
    rules = [
        row for row in store.rows("data_retention")
        if row.get("目标数据集") == dataset and row.get("status") == "已生效"
    ]
    return sorted(rules, key=retention_days)


def max_retention_days(rules: list[dict[str, Any]]) -> int:
    return max((retention_days(rule) for rule in rules), default=0)


def inventory_partitions(store: Any, dataset: str) -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows(INVENTORY_TABLE)
        if row.get("数据集") == dataset
    ]
    return sorted(rows, key=lambda row: str(row.get("分区日期", "")))


def evaluate_partition(partition: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any]:
    """按生效规则逐条核对单个分区，给出处置结论和依据。

    结论优先级：全部覆盖规则的留存期都届满且存在到期/删除类规则
    → 物理删除；否则命中转冷类规则 → 转冷；其余 → 保留中。
    """
    age = partition_age(partition)
    checks: list[dict[str, Any]] = []
    for rule in rules:
        if not scope_covers(rule, partition):
            continue
        days = retention_days(rule)
        checks.append({
            "规则id": rule.get("id"),
            "规则名称": rule.get("规则名称"),
            "适用范围": rule_scope(rule),
            "保留天数": days,
            "清理方式": str(rule.get("清理方式", "")),
            "截止日期": rule_cutoff(rule).isoformat(),
            "是否命中": age > days,
        })
    hits = [check for check in checks if check["是否命中"]]
    max_days = max((check["保留天数"] for check in checks), default=0)
    all_expired = bool(checks) and all(check["是否命中"] for check in checks)
    expire_hit = any(check["清理方式"] in EXPIRE_METHODS for check in hits)
    tier_hit = any(check["清理方式"] in TIER_METHODS for check in hits)

    if expire_hit and all_expired:
        conclusion = CONCLUSION_DELETE
    elif tier_hit:
        conclusion = CONCLUSION_TO_COLD
    else:
        conclusion = CONCLUSION_KEEP

    return {
        "年龄天数": age,
        "最长保留天数": max_days,
        "命中规则": hits,
        "核对明细": checks,
        "处置结论": conclusion,
        "处置依据": _basis_text(age, checks, hits, conclusion, max_days),
    }


def _format_check(check: dict[str, Any]) -> str:
    return (
        f"「{check['规则名称']}」（{check['适用范围']}，保留 {check['保留天数']} 天，"
        f"截止 {check['截止日期']}）"
    )


def _basis_text(
    age: int,
    checks: list[dict[str, Any]],
    hits: list[dict[str, Any]],
    conclusion: str,
    max_days: int,
) -> str:
    if conclusion == CONCLUSION_DELETE:
        return (
            f"已保存 {age} 天，超出最长保留期 {max_days} 天；命中"
            + "、".join(_format_check(hit) for hit in hits)
            + "，全部生效规则留存期届满，可物理删除"
        )
    if conclusion == CONCLUSION_TO_COLD:
        tier_hits = [h for h in hits if h["清理方式"] in TIER_METHODS]
        retaining = [c for c in checks if not c["是否命中"]]
        text = (
            f"已保存 {age} 天，命中"
            + "、".join(_format_check(hit) for hit in tier_hits)
            + "：副本应转冷"
        )
        if retaining:
            text += (
                "；" + "、".join(_format_check(c) for c in retaining)
                + "仍在留存期内，转冷后须保持完整可回查，不得物理删除"
            )
        return text
    if not checks:
        return f"已保存 {age} 天，没有覆盖该分区的生效规则，保持现状"
    expired_marks = [h for h in hits if h["清理方式"] in EXPIRE_METHODS]
    retaining = [c for c in checks if not c["是否命中"]]
    if expired_marks and retaining:
        return (
            f"已保存 {age} 天，"
            + "、".join(_format_check(h) for h in expired_marks)
            + "已到期，但"
            + "、".join(_format_check(c) for c in retaining)
            + "仍在留存期内，须完整保留并保持可回查"
        )
    nearest = min(checks, key=lambda c: c["保留天数"])
    return (
        f"已保存 {age} 天，未命中任何生效规则；按"
        + _format_check(nearest)
        + "，留存期内保持现状、完整可回查"
    )


def build_plan_rows(store: Any, dataset: str) -> list[dict[str, Any]]:
    """根据生效规则和分区台账，逐分区计算处置清单行。"""
    rules = effective_rules(store, dataset)
    dataset_max = max_retention_days(rules)
    rows: list[dict[str, Any]] = []
    for part in inventory_partitions(store, dataset):
        verdict = evaluate_partition(part, rules)
        conclusion = verdict["处置结论"]
        basis = verdict["处置依据"]
        state = str(part.get("处置状态") or "未处置")
        if state == "已删除":
            status = STATUS_DELETED
            conclusion = CONCLUSION_DELETE
            basis = (
                f"已于 {part.get('处置时间', '')} 按清单物理删除"
                f"（作业 {part.get('处置作业', '')}），留存期届满，不可回查"
            )
        elif state == "已转冷" and conclusion != CONCLUSION_DELETE:
            # 已完成的迁移直接继承；只有留存期也届满时才升级为待删除
            status = STATUS_COLD
        elif conclusion == CONCLUSION_KEEP:
            status = STATUS_KEEP
        else:
            status = STATUS_PENDING
        rows.append({
            "id": 0,
            "数据集": dataset,
            "分区日期": part.get("分区日期"),
            "年龄天数": verdict["年龄天数"],
            "存储层": part.get("存储层"),
            "处置结论": conclusion,
            "处置依据": basis,
            "命中规则": [
                {"规则名称": h["规则名称"], "截止日期": h["截止日期"], "清理方式": h["清理方式"]}
                for h in verdict["命中规则"]
            ],
            "最长保留天数": verdict["最长保留天数"] or dataset_max,
            "状态": status,
            "副本位置": part.get("副本位置", ""),
            "执行时间": part.get("处置时间", ""),
            "执行作业": part.get("处置作业", ""),
            "清单生成时间": EXECUTION_TIME,
        })
    return rows


def sync_plan_table(store: Any, dataset: str) -> list[dict[str, Any]]:
    """重建指定数据集的处置清单并持久化。

    已确认的转冷/删除结果从分区台账继承，重复生成不会推翻
    已完成的处置，也不会丢失执行时间和执行作业。
    """
    rows = build_plan_rows(store, dataset)
    table = store.rows(PLAN_TABLE)
    previous = {(row.get("数据集"), row.get("分区日期")): row for row in table}
    next_id = max((int(row.get("id", 0)) for row in table), default=0)
    for row in rows:
        prev = previous.get((row["数据集"], row["分区日期"]))
        if prev is None:
            next_id += 1
            row["id"] = next_id
            continue
        row["id"] = prev.get("id", row["id"])
        if prev.get("状态") in DONE_STATES and prev.get("状态") == row["状态"]:
            row["执行时间"] = row["执行时间"] or prev.get("执行时间", "")
            row["执行作业"] = row["执行作业"] or prev.get("执行作业", "")
    table[:] = [row for row in table if row.get("数据集") != dataset] + rows
    return rows


def plan_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """汇总处置清单：状态计数和本次可执行的日期范围。"""
    def _range(items: list[dict[str, Any]]) -> dict[str, Any] | None:
        days = sorted(str(item["分区日期"]) for item in items)
        if not days:
            return None
        return {"start": days[0], "end": days[-1], "count": len(days)}

    pending_cold = [
        row for row in rows
        if row["状态"] == STATUS_PENDING and row["处置结论"] == CONCLUSION_TO_COLD
    ]
    pending_delete = [
        row for row in rows
        if row["状态"] == STATUS_PENDING and row["处置结论"] == CONCLUSION_DELETE
    ]
    status_counts: dict[str, int] = {}
    for row in rows:
        status_counts[row["状态"]] = status_counts.get(row["状态"], 0) + 1
    conclusion_counts: dict[str, int] = {}
    for row in rows:
        conclusion_counts[row["处置结论"]] = conclusion_counts.get(row["处置结论"], 0) + 1
    return {
        "total_partitions": len(rows),
        "status_counts": status_counts,
        "conclusion_counts": conclusion_counts,
        "pending_cold_range": _range(pending_cold),
        "pending_delete_range": _range(pending_delete),
        "deletable_partitions": [row["分区日期"] for row in pending_delete],
        "done_counts": {state: status_counts.get(state, 0) for state in DONE_STATES},
    }


def safety_violations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """留存期安全校验：留存期内分区必须完整可回查，且不得进入物理删除清单。"""
    violations: list[dict[str, Any]] = []
    for row in rows:
        if row["年龄天数"] > row["最长保留天数"]:
            continue
        if row["状态"] == STATUS_DELETED:
            violations.append({"分区日期": row["分区日期"], "问题": "留存期内分区被物理删除"})
        elif not row.get("副本位置"):
            violations.append({"分区日期": row["分区日期"], "问题": "留存期内分区缺少副本位置，无法回查"})
        if row["状态"] == STATUS_PENDING and row["处置结论"] == CONCLUSION_DELETE:
            violations.append({"分区日期": row["分区日期"], "问题": "留存期内分区误入物理删除清单"})
    return violations
