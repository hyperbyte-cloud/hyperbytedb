# Copyright 2026 Hyperbyte Cloud
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env python3
"""Verify relative markdown links in docs/ and README.md point to existing files.

Only checks same-repository paths ending in .md (or containing #fragment after .md).
Skips http(s), mailto, and absolute paths.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LINK_PAT = re.compile(r"\[[^\]]*\]\(([^)]+)\)")


def resolve_target(current_file: Path, target: str) -> Path | None:
    t = target.strip()
    if not t or t.startswith(("#", "http://", "https://", "mailto:")):
        return None
    if t.startswith("/"):
        return None
    t = t.split("#", 1)[0]
    if not t.endswith(".md"):
        return None
    return (current_file.parent / t).resolve()


def main() -> int:
    roots = [REPO_ROOT / "README.md", *sorted((REPO_ROOT / "docs").rglob("*.md"))]
    errors: list[str] = []
    for md in roots:
        if not md.is_file():
            continue
        text = md.read_text(encoding="utf-8", errors="replace")
        for m in LINK_PAT.finditer(text):
            raw = m.group(1)
            if raw.startswith("<") and raw.endswith(">"):
                raw = raw[1:-1]
            resolved = resolve_target(md, raw)
            if resolved is not None:
                try:
                    resolved.relative_to(REPO_ROOT)
                except ValueError:
                    # e.g. ../hypersim (sibling repo) — not verified here
                    continue
                if not resolved.is_file():
                    rel = md.relative_to(REPO_ROOT)
                    errors.append(
                        f"{rel}: broken link to {raw!r} (expected {resolved.relative_to(REPO_ROOT)})"
                    )
    if errors:
        print("check_doc_links: broken relative .md links:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1
    print(f"check_doc_links: OK ({len(roots)} files scanned)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
