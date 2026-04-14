"""
yuanbao_markdown.py - Fence-aware Markdown text chunking utilities.

Migrated from TypeScript (markdown-stream.ts) in yuanbao-openclaw-plugin.
Provides pure-function helpers for splitting Markdown text into safe chunks
that never break inside code fences or table rows.
"""

from __future__ import annotations

import re


# ============ Text state detection ============

def has_unclosed_fence(text: str) -> bool:
    """
    检测文本中是否有未闭合的代码块围栏。

    逐行扫描，遇到以 ``` 开头的行时切换 in/out 状态。
    奇数次切换说明存在未闭合的围栏。

    Args:
        text: 待检测的 Markdown 文本

    Returns:
        若文本以未闭合围栏结尾则返回 True，否则返回 False
    """
    in_fence = False
    for line in text.split('\n'):
        if line.startswith('```'):
            in_fence = not in_fence
    return in_fence


def ends_with_table_row(text: str) -> bool:
    """
    检测文本是否以表格行结尾（最后一个非空行以 | 开头且以 | 结尾）。

    Args:
        text: 待检测的文本

    Returns:
        若最后一个非空行是表格行则返回 True
    """
    trimmed = text.rstrip()
    if not trimmed:
        return False
    last_line = trimmed.split('\n')[-1].strip()
    return last_line.startswith('|') and last_line.endswith('|')


# ============ Paragraph boundary splitting ============

def split_at_paragraph_boundary(text: str, max_chars: int) -> tuple[str, str]:
    """
    在不超过 max_chars 的前提下，找到最近的段落边界切割点，返回 (前段, 后段)。

    切割优先级：
    1. 空行（段落边界）
    2. 句号/问号/感叹号（中英文）后的换行
    3. 最后一个换行
    4. 强制在 max_chars 处切割

    Args:
        text: 待切割文本
        max_chars: 最大字符数上限

    Returns:
        (head, tail) 元组，head 是切割前段，tail 是切割后段，满足 head + tail == text
    """
    if len(text) <= max_chars:
        return text, ''

    window = text[:max_chars]

    # 1. 优先找最后一个空行（\n\n）作为段落边界
    pos = window.rfind('\n\n')
    if pos > 0:
        return text[:pos + 2], text[pos + 2:]

    # 2. 其次找最后一个句子结束符后的换行
    sentence_end_re = re.compile(r'[。！？.!?]\n')
    best_pos = -1
    for m in sentence_end_re.finditer(window):
        best_pos = m.end()
    if best_pos > 0:
        return text[:best_pos], text[best_pos:]

    # 3. 退而求其次：找最后一个换行
    pos = window.rfind('\n')
    if pos > 0:
        return text[:pos + 1], text[pos + 1:]

    # 4. 实在没有合法切割点，强制在 max_chars 处切割
    return text[:max_chars], text[max_chars:]


# ============ Atom type detection (internal) ============

def _is_fence_atom(text: str) -> bool:
    """判断原子块是否是代码块（以 ``` 开头）。"""
    return text.lstrip().startswith('```')


def _is_table_atom(text: str) -> bool:
    """判断原子块是否是表格（第一行以 | 开头）。"""
    first_line = text.split('\n')[0].strip()
    return first_line.startswith('|') and first_line.endswith('|')


# ============ Atom splitting (internal) ============

def _split_into_atoms(text: str) -> list[str]:
    """
    将文本拆分为"原子块"列表，每个原子块是不可分割的逻辑单元：

    - 代码块（fence）：从 ``` 开到对应 ``` 关闭的整段（含首尾 fence 行）
    - 表格：连续的 |...| 行组成的整段
    - 普通段落：以空行分隔的普通文本段

    空行作为分隔符，不加入任何原子块。

    Args:
        text: 待拆分的 Markdown 文本

    Returns:
        原子块字符串列表（均非空）
    """
    lines = text.split('\n')
    atoms: list[str] = []

    current_lines: list[str] = []
    in_fence = False

    def _is_table_line(line: str) -> bool:
        stripped = line.strip()
        return stripped.startswith('|') and stripped.endswith('|')

    def _flush_current() -> None:
        if current_lines:
            atom = '\n'.join(current_lines)
            if atom.strip():
                atoms.append(atom)
            current_lines.clear()

    for line in lines:
        if in_fence:
            # 在 fence 内：所有行都加入当前原子块
            current_lines.append(line)
            if line.startswith('```') and len(current_lines) > 1:
                # fence 关闭行（不是开始行）
                in_fence = False
                _flush_current()
        elif line.startswith('```'):
            # fence 开始：先 flush 当前普通段落，再开始 fence 原子块
            _flush_current()
            in_fence = True
            current_lines.append(line)
        elif _is_table_line(line):
            # 表格行：如果当前原子块不是表格，先 flush
            if current_lines and not _is_table_line(current_lines[-1]):
                _flush_current()
            current_lines.append(line)
        elif line.strip() == '':
            # 空行：结束当前原子块
            _flush_current()
        else:
            # 普通文本行：如果当前原子块是表格，先 flush
            if current_lines and _is_table_line(current_lines[-1]):
                _flush_current()
            current_lines.append(line)

    # 处理文件末尾未 flush 的内容（含未闭合 fence 的情况）
    _flush_current()

    return atoms


# ============ Core chunking ============

def chunk_markdown_text(text: str, max_chars: int = 3000) -> list[str]:
    """
    将 Markdown 文本按 max_chars 切割为多个片段。

    保证：
    - 每个片段 <= max_chars 字符（除非单段本身超过限制，如超大代码块/表格）
    - 代码块（```...```）不在中间被切断
    - 表格行不在中间被切断（表格作为原子块整体输出）
    - 在段落边界切割（空行、句号后等）

    算法：两阶段——先拆原子块，再贪心合并。

    Phase 1: 将文本拆分为不可分割的原子块
      - 代码块：整段不可切割
      - 表格：整段不可切割
      - 普通段落：以空行分隔的文本段（可在段落边界二次切割）

    Phase 2: 贪心合并
      - 从左到右将原子块合并到当前 chunk
      - 当加入下一个原子块会超过 max_chars 时，flush 当前 chunk
      - 如果单个原子块是 fence/table 且 > max_chars，整块输出（允许超限，不切割）
      - 如果单个原子块是普通文本且 > max_chars，允许段落边界切割

    Args:
        text: 待切割的 Markdown 文本
        max_chars: 每片段最大字符数，默认 3000

    Returns:
        切割后的文本片段列表（非空）
    """
    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    # Phase 1: 提取原子块
    atoms = _split_into_atoms(text)

    # Phase 2: 贪心合并
    # indivisible_set: 记录哪些 chunk index 是不可分割的超大 fence/table 原子块
    chunks: list[str] = []
    indivisible_set: set[int] = set()
    current_parts: list[str] = []
    current_len = 0

    def _flush_parts() -> None:
        if current_parts:
            chunks.append('\n\n'.join(current_parts))

    for atom in atoms:
        atom_len = len(atom)
        # 计算加入此原子块后的总长度（parts 之间用 '\n\n' 连接）
        sep_len = 2 * len(current_parts)  # 每个 '\n\n' 占 2 字符
        projected_len = current_len + sep_len + atom_len

        if projected_len > max_chars and current_parts:
            # flush 当前 chunk
            _flush_parts()
            current_parts = []
            current_len = 0

        # 如果此时 current_parts 为空，且 atom 是 fence/table 且 > max_chars
        # 说明这是不可分割的超大原子块（代码块/表格），直接整块输出，不再后处理切割
        if (not current_parts
                and atom_len > max_chars
                and (_is_fence_atom(atom) or _is_table_atom(atom))):
            indivisible_set.add(len(chunks))
            chunks.append(atom)
            # current_parts 保持空，继续下一个 atom
            continue

        current_parts.append(atom)
        current_len += atom_len

    _flush_parts()

    # Phase 3: 后处理
    # 对仍超长的 chunk 做段落边界切割，跳过不可分割的 fence/table 块
    result: list[str] = []
    for idx, chunk in enumerate(chunks):
        if len(chunk) <= max_chars:
            result.append(chunk)
            continue

        # 不可分割的超大 fence/table 原子块 — 整块输出
        if idx in indivisible_set:
            result.append(chunk)
            continue

        # 含未闭合 fence — 整块输出（graceful degradation）
        if has_unclosed_fence(chunk):
            result.append(chunk)
            continue

        # 安全切割（普通文本段落组合超限，或超大普通文本原子块）
        remaining = chunk
        while len(remaining) > max_chars:
            head, remaining = split_at_paragraph_boundary(remaining, max_chars)
            if not head:
                head, remaining = remaining[:max_chars], remaining[max_chars:]
            if head:
                result.append(head)
        if remaining:
            result.append(remaining)

    return [c for c in result if c]
