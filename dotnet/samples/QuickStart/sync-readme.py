#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
"""Copies the code regions of samples/QuickStart into dotnet/README.md and the repository's root README.md.

A region in a sample file:

    // <snippet:open-sync>
    ...code...
    // </snippet:open-sync>

is copied (dedented) into the fenced block that follows `<!-- snippet:open-sync -->` in each README. dotnet/README.md
must use every snippet; the root README uses a selection.

Usage (from dotnet/):  python3 samples/QuickStart/sync-readme.py [--check]
--check exits with status 1 (and changes nothing) when a README is out of date.
"""
import pathlib
import re
import sys
import textwrap

HERE = pathlib.Path(__file__).resolve().parent
README = HERE.parent.parent / "README.md"
ROOT_README = HERE.parent.parent.parent / "README.md"
REGION = re.compile(r"^[ \t]*// <snippet:([\w-]+)>[ \t]*\n(.*?)^[ \t]*// </snippet:\1>[ \t]*$", re.S | re.M)
BLOCK = re.compile(r"(<!-- snippet:([\w-]+) -->\n```csharp\n)(.*?)(```)", re.S)


def main() -> int:
    snippets = {}
    for path in sorted(HERE.glob("*.cs")):
        for m in REGION.finditer(path.read_text(encoding="utf-8")):
            if m.group(1) in snippets:
                sys.exit(f"duplicate snippet {m.group(1)} in {path.name}")
            snippets[m.group(1)] = textwrap.dedent(m.group(2))

    status = 0
    for path, require_all in ((README, True), (ROOT_README, False)):
        status |= sync(path, snippets, require_all)
    return status


def sync(path: pathlib.Path, snippets: dict, require_all: bool) -> int:
    name_shown = path.relative_to(HERE.parent.parent.parent).as_posix()
    readme = path.read_text(encoding="utf-8")
    used = set()

    def replace(m: re.Match) -> str:
        name = m.group(2)
        if name not in snippets:
            sys.exit(f"{name_shown} references unknown snippet {name}")
        used.add(name)
        return m.group(1) + snippets[name] + m.group(4)

    updated = BLOCK.sub(replace, readme)
    unused = sorted(set(snippets) - used)
    if require_all and unused:
        print(f"snippets not used by {name_shown}: " + ", ".join(unused))
    if updated == readme:
        print(f"{name_shown} is up to date")
        return 0
    if "--check" in sys.argv[1:]:
        print(f"{name_shown} is out of date: run python3 samples/QuickStart/sync-readme.py")
        return 1
    path.write_text(updated, encoding="utf-8")
    print(f"{name_shown} updated")
    return 0

if __name__ == "__main__":
    sys.exit(main())
