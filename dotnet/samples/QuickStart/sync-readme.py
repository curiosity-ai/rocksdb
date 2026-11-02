#!/usr/bin/env python3
#  Copyright (c) Meta Platforms, Inc. and affiliates.
#  This source code is licensed under both the GPLv2 (found in the
#  COPYING file in the root directory) and Apache 2.0 License
#  (found in the LICENSE.Apache file in the root directory).
"""Copies the code regions of samples/QuickStart into dotnet/README.md.

A region in a sample file:

    // <snippet:open-sync>
    ...code...
    // </snippet:open-sync>

is copied (dedented) into the fenced block that follows `<!-- snippet:open-sync -->` in the README.

Usage (from dotnet/):  python3 samples/QuickStart/sync-readme.py [--check]
--check exits with status 1 (and changes nothing) when the README is out of date.
"""
import pathlib
import re
import sys
import textwrap

HERE = pathlib.Path(__file__).resolve().parent
README = HERE.parent.parent / "README.md"
REGION = re.compile(r"^[ \t]*// <snippet:([\w-]+)>[ \t]*\n(.*?)^[ \t]*// </snippet:\1>[ \t]*$", re.S | re.M)
BLOCK = re.compile(r"(<!-- snippet:([\w-]+) -->\n```csharp\n)(.*?)(```)", re.S)


def main() -> int:
    snippets = {}
    for path in sorted(HERE.glob("*.cs")):
        for m in REGION.finditer(path.read_text(encoding="utf-8")):
            if m.group(1) in snippets:
                sys.exit(f"duplicate snippet {m.group(1)} in {path.name}")
            snippets[m.group(1)] = textwrap.dedent(m.group(2))

    readme = README.read_text(encoding="utf-8")
    used = set()

    def replace(m: re.Match) -> str:
        name = m.group(2)
        if name not in snippets:
            sys.exit(f"README references unknown snippet {name}")
        used.add(name)
        return m.group(1) + snippets[name] + m.group(4)

    updated = BLOCK.sub(replace, readme)
    unused = sorted(set(snippets) - used)
    if unused:
        print("snippets not used by the README: " + ", ".join(unused))
    if updated == readme:
        print("README.md is up to date")
        return 0
    if "--check" in sys.argv[1:]:
        print("README.md is out of date: run python3 samples/QuickStart/sync-readme.py")
        return 1
    README.write_text(updated, encoding="utf-8")
    print("README.md updated")
    return 0


if __name__ == "__main__":
    sys.exit(main())
