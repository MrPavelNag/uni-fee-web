#!/usr/bin/env python3
"""Lightweight guardrails for inline JavaScript in webapp/main.py.

This is intentionally conservative and dependency-free:
- checks bracket balance ((), [], {}) outside strings/comments
- checks duplicated const/let declarations in the same block (heuristic)

It is not a full JavaScript parser, but it catches common fatal mistakes
that can break the whole page into "HTML only" mode.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Issue:
    script_idx: int
    line: int
    message: str


SCRIPT_RE = re.compile(r"<script>(.*?)</script>", re.S)
IDENT_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")


def _extract_scripts(text: str) -> list[str]:
    return [m.group(1) for m in SCRIPT_RE.finditer(text)]


def _near_for_initializer(src: str, pos: int) -> bool:
    left = src[max(0, pos - 32) : pos]
    return bool(re.search(r"for\s*\([^)]*$", left))


def _scan_script(script_idx: int, src: str) -> list[Issue]:
    issues: list[Issue] = []
    line = 1
    i = 0
    n = len(src)

    in_str: str | None = None
    escaped = False
    in_line_comment = False
    in_block_comment = False

    stack: list[tuple[str, int]] = []
    scopes: list[set[str]] = [set()]

    def push_scope() -> None:
        scopes.append(set())

    def pop_scope() -> None:
        if len(scopes) > 1:
            scopes.pop()

    while i < n:
        ch = src[i]
        nxt = src[i + 1] if i + 1 < n else ""

        if ch == "\n":
            line += 1
            in_line_comment = False

        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == in_str:
                in_str = None
            i += 1
            continue

        if in_line_comment:
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if ch == "/" and nxt == "/":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue
        if ch in ("'", '"', "`"):
            in_str = ch
            i += 1
            continue

        if ch in "([{":
            stack.append((ch, line))
            if ch == "{":
                push_scope()
            i += 1
            continue

        if ch in ")]}":
            if not stack:
                issues.append(Issue(script_idx, line, f"Unmatched closing '{ch}'"))
                i += 1
                continue
            opener, open_line = stack.pop()
            pair_ok = (opener, ch) in {("(", ")"), ("[", "]"), ("{", "}")}
            if not pair_ok:
                issues.append(
                    Issue(
                        script_idx,
                        line,
                        f"Mismatched bracket '{opener}' (opened line {open_line}) with '{ch}'",
                    )
                )
            if ch == "}":
                pop_scope()
            i += 1
            continue

        if src.startswith("const ", i) or src.startswith("let ", i):
            kw = "const" if src.startswith("const ", i) else "let"
            if _near_for_initializer(src, i):
                i += len(kw)
                continue
            j = i + len(kw)
            while j < n and src[j].isspace():
                j += 1
            m = IDENT_RE.match(src[j:])
            if m:
                name = m.group(0)
                current_scope = scopes[-1]
                if name in current_scope:
                    issues.append(
                        Issue(
                            script_idx,
                            line,
                            f"Duplicate {kw} declaration in same scope: '{name}'",
                        )
                    )
                else:
                    current_scope.add(name)
            i = j + (len(m.group(0)) if m else 0)
            continue

        i += 1

    for opener, open_line in stack:
        issues.append(Issue(script_idx, open_line, f"Unclosed bracket '{opener}'"))
    if in_str:
        issues.append(Issue(script_idx, line, "Unclosed string literal"))
    if in_block_comment:
        issues.append(Issue(script_idx, line, "Unclosed block comment"))

    return issues


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("webapp/main.py")
    if not target.exists():
        print(f"[inline-js-check] file not found: {target}", file=sys.stderr)
        return 2

    text = target.read_text(encoding="utf-8")
    scripts = _extract_scripts(text)
    if not scripts:
        print(f"[inline-js-check] no <script> blocks in {target}")
        return 0

    issues: list[Issue] = []
    for idx, src in enumerate(scripts, start=1):
        issues.extend(_scan_script(idx, src))

    if not issues:
        print(f"[inline-js-check] OK: {len(scripts)} script blocks checked in {target}")
        return 0

    print(f"[inline-js-check] FAILED: {len(issues)} issue(s) in {target}", file=sys.stderr)
    for item in issues:
        print(
            f"  - script #{item.script_idx}, line ~{item.line}: {item.message}",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
