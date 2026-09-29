"""不可混淆的类型化编号。

编号 = 类型前缀 + 不透明后缀，解析时校验前缀，防止把作业编号
当作树木编号、把批次编号当作交接编号这类跨实体混用。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# 各实体的稳定前缀，与 contracts/domain.json 的 id_prefixes 对齐。
PREFIXES: dict[str, str] = {
    "cultivar": "cultivar-",
    "tree_record": "tree-",
    "annual_assessment": "assess-",
    "picking_range": "range-",
    "crew": "crew-",
    "harvest_operation": "op-",
    "harvest_lot": "batch-",
    "damage_observation": "obs-",
    "custody_transfer": "xfer-",
    "care_action": "care-",
}

_TOKEN_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,63}$")


class IdError(ValueError):
    """编号缺失、后缀非法或类型不匹配时抛出。"""


@dataclass(frozen=True)
class Id:
    kind: str
    token: str

    @property
    def value(self) -> str:
        return PREFIXES[self.kind] + self.token

    def __str__(self) -> str:
        return self.value


def make_id(kind: str, token: str) -> Id:
    """按类型构造编号；token 为调用方持有的稳定后缀（如业务序号或 UUID）。"""
    if kind not in PREFIXES:
        raise IdError(f"未知的编号类型: {kind}")
    token = str(token).strip()
    if not _TOKEN_RE.match(token):
        raise IdError(f"编号后缀非法: {token!r}")
    return Id(kind, token)


def parse_id(raw: str, *expected_kinds: str) -> Id:
    """解析编号并断言它属于期望类型中的一种。"""
    if not isinstance(raw, str) or not raw.strip():
        raise IdError("编号必须是非空字符串")
    matched: list[str] = []
    for kind, prefix in PREFIXES.items():
        if raw.startswith(prefix):
            matched.append(kind)
    # 前缀之间不互为前缀（batch-/care-/…），正常只命中一个。
    if len(matched) != 1:
        raise IdError(f"无法识别编号类型: {raw}")
    kind = matched[0]
    token = raw[len(PREFIXES[kind]):]
    parsed = make_id(kind, token)
    if expected_kinds and kind not in expected_kinds:
        raise IdError(
            f"编号类型不匹配: 期望 {sorted(expected_kinds)}，实际 {kind}（{raw}）"
        )
    return parsed
