"""Build a SHA-bound GitHub review payload from validated AI findings."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from collections.abc import Collection
from pathlib import Path
from typing import Any

from review_history import (
    DEFAULT_TRUSTED_BOT_LOGINS,
    canonical_finding_body,
    is_duplicate_review,
    previous_inline_comments,
)
from review_publish import INLINE_FEEDBACK_FOOTER, format_review_body as _format_review_body


MAX_INLINE_FINDINGS = 12
INLINE_FINDING_FIELDS = ("id", "path", "line", "side", "body")
INLINE_FINDING_SIDES = ("LEFT", "RIGHT")
_FINDING_HEADING_MARKER = "###"
_FINDING_HEADING_TEMPLATE = f"{_FINDING_HEADING_MARKER} <ID>"
_REVIEW_SECTION_NAMES = ("Blocking issues", "Non-blocking notes", "Summary")
_FINDING_ID = re.compile(r"(?:Blocker|Nit)[1-9][0-9]*")
_FINDING_HEADING = re.compile(
    rf"^{re.escape(_FINDING_HEADING_MARKER)}\s+({_FINDING_ID.pattern})\b"
)
_SECTION_NAME_PATTERN = "|".join(re.escape(name) for name in _REVIEW_SECTION_NAMES)
_SECTION_HEADING = re.compile(
    rf"^(?:(?:##\s+)(?:{_SECTION_NAME_PATTERN})\s*:?|"
    rf"(?:\d+\.\s+\*\*)(?:{_SECTION_NAME_PATTERN})\*\*\s*:?|"
    rf"(?:{_SECTION_NAME_PATTERN})\s*:)\s*$",
    re.IGNORECASE,
)
_FINDING_GROUP_NAME_PATTERN = "|".join(
    re.escape(name) for name in _REVIEW_SECTION_NAMES[:2]
)
_FINDING_GROUP_HEADING = re.compile(
    rf"^(?:(?:##\s+)(?:{_FINDING_GROUP_NAME_PATTERN})\s*:?|"
    rf"(?:\d+\.\s+\*\*)(?:{_FINDING_GROUP_NAME_PATTERN})\*\*\s*:?|"
    rf"(?:{_FINDING_GROUP_NAME_PATTERN})\s*:)\s*$",
    re.IGNORECASE,
)
_CODE_FENCE = re.compile(r"^\s{0,3}(`{3,}|~{3,})")
_HUNK_HEADER = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@"
)


def extract_inline_findings(
    review: str, marker: str
) -> tuple[str, list[dict[str, Any]]]:
    """Remove and decode the per-run inline-finding block from a review."""
    start, end = _inline_markers(marker)
    if review.count(start) != 1 or review.count(end) != 1:
        raise ValueError("inline review output must contain one location block")

    start_index = review.index(start)
    payload_start = start_index + len(start)
    end_index = review.index(end, payload_start)
    document = json.loads(review[payload_start:end_index].strip())
    if not isinstance(document, dict) or set(document) != {"findings"}:
        raise ValueError("inline review document must contain only 'findings'")
    findings = document["findings"]
    if not isinstance(findings, list):
        raise ValueError("inline review findings must be a list")
    if len(findings) > MAX_INLINE_FINDINGS:
        raise ValueError(
            f"inline review contains more than {MAX_INLINE_FINDINGS} findings"
        )

    clean_review = (review[:start_index] + review[end_index + len(end) :]).strip()
    if not clean_review:
        raise ValueError("inline review has no human-readable content")
    return clean_review, findings


def inline_prompt_instructions(marker: str) -> str:
    """Build the prompt contract for machine-readable inline findings."""
    start, end = _inline_markers(marker)
    example = json.dumps(
        {
            "findings": [
                dict(
                    zip(
                        INLINE_FINDING_FIELDS,
                        (
                            "Nit1",
                            "kernel/src/file.rs",
                            10,
                            "RIGHT",
                            (
                                "Complete, concise finding with failure mode, "
                                "reviewer attribution, and suggested fix."
                            ),
                        ),
                        strict=True,
                    )
                )
            ]
        },
        separators=(",", ":"),
    )
    return f"""

This review will also be published as GitHub inline comments. After the
human-readable review and before the final end marker, emit this exact
machine-readable block:

{start}
{example}
{end}

Include entries for up to {MAX_INLINE_FINDINGS} Blocker/Nit findings, prioritizing
blockers and then the most useful notes. Findings omitted from this block remain
in the collapsed review. Start every human-readable finding on its own Markdown
heading matching `{_FINDING_HEADING_TEMPLATE}`, using the same ID in this block.
Keep the Summary to an overall assessment rather than repeating finding details.
Use IDs matching `{_FINDING_ID.pattern}` (for example, `Blocker1` or `Nit1`) and
do not add an entry for the Summary. Use the repository-relative path with no
backticks. Use {INLINE_FINDING_SIDES[1]} and the head-file line
number for additions or context; use {INLINE_FINDING_SIDES[0]} and the base-file
line number only for deleted lines. The location must occur in the supplied
unified diff. Use an empty findings list when there are no findings. Do not wrap
the JSON in a Markdown code fence.
"""


def diff_positions(diff: str) -> set[tuple[str, int, str]]:
    """Return GitHub-reviewable ``(path, line, side)`` positions from a diff."""
    positions: set[tuple[str, int, str]] = set()
    old_path: str | None = None
    new_path: str | None = None
    old_line: int | None = None
    new_line: int | None = None
    in_hunk = False

    for raw_line in diff.splitlines():
        if raw_line.startswith("diff --git "):
            old_path = None
            new_path = None
            old_line = None
            new_line = None
            in_hunk = False
            continue
        if not in_hunk and raw_line.startswith("--- "):
            old_path = _diff_path(raw_line[4:])
            continue
        if not in_hunk and raw_line.startswith("+++ "):
            new_path = _diff_path(raw_line[4:])
            continue

        hunk = _HUNK_HEADER.match(raw_line)
        if hunk:
            old_line = int(hunk.group(1))
            new_line = int(hunk.group(3))
            in_hunk = True
            continue
        if old_line is None or new_line is None:
            continue
        if raw_line.startswith("\\ No newline at end of file"):
            continue

        review_path = new_path or old_path
        if review_path is None:
            continue
        if raw_line.startswith("+"):
            positions.add((review_path, new_line, "RIGHT"))
            new_line += 1
        elif raw_line.startswith("-"):
            positions.add((review_path, old_line, "LEFT"))
            old_line += 1
        else:
            positions.add((review_path, new_line, "RIGHT"))
            old_line += 1
            new_line += 1

    return positions


def build_review_payload(
    *,
    review: str,
    findings: list[dict[str, Any]],
    diff: str,
    head_sha: str,
    run_url: str,
    history: Any = None,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
) -> tuple[dict[str, Any], list[str], list[str]]:
    """Build a non-blocking review and classify unmapped and duplicate findings."""
    if re.fullmatch(r"[0-9a-f]{40}", head_sha) is None:
        raise ValueError("head SHA must be a 40-character lowercase hex value")

    allowed_positions = diff_positions(diff)
    comments: list[dict[str, Any]] = []
    unmapped: list[str] = []
    duplicates: list[str] = []
    seen_ids: set[str] = set()
    headings = _review_boundaries(review)
    review_ids = {
        finding.group(1)
        for _, line in headings
        if (finding := _FINDING_HEADING.match(line)) is not None
    }
    prior_comments = {
        (
            comment["path"],
            comment["line"],
            comment["side"],
            canonical_finding_body(comment["body"]),
        )
        for comment in previous_inline_comments(history, trusted_bot_logins)
    }
    omitted_ids: set[str] = set()

    for index, finding in enumerate(findings, start=1):
        try:
            finding_id, path, line, side, body = _validate_finding(finding)
        except ValueError:
            unmapped.append(f"entry {index}")
            continue
        if finding_id in seen_ids:
            unmapped.append(finding_id)
            continue
        seen_ids.add(finding_id)

        if (path, line, side) not in allowed_positions:
            unmapped.append(finding_id)
            continue
        if (path, line, side, canonical_finding_body(body)) in prior_comments:
            duplicates.append(finding_id)
            if finding_id in review_ids:
                omitted_ids.add(finding_id)
            continue
        if finding_id in review_ids:
            omitted_ids.add(finding_id)
        comments.append(
            {
                "path": path,
                "line": line,
                "side": side,
                "body": f"**{finding_id}** {body}{INLINE_FEEDBACK_FOOTER}",
            }
        )

    return (
        {
            "body": _format_review_body(
                _remove_finding_sections(review, omitted_ids),
                run_url,
                collapsed=True,
            ),
            "commit_id": head_sha,
            "event": "COMMENT",
            "comments": comments,
        },
        unmapped,
        duplicates,
    )


def _remove_finding_sections(review: str, finding_ids: set[str]) -> str:
    """Remove finding sections that are published inline or already present."""
    if not finding_ids:
        return review

    headings, unclosed_fence_start, hidden_finding_offsets = _parse_review_boundaries(
        review
    )
    finding_id_counts = Counter(
        finding.group(1)
        for _, line in headings
        if (finding := _FINDING_HEADING.match(line)) is not None
    )
    ranges: list[tuple[int, int]] = []
    for index, (start, line) in enumerate(headings):
        finding = _FINDING_HEADING.match(line)
        if finding is None or finding.group(1) not in finding_ids:
            continue
        if finding_id_counts[finding.group(1)] != 1:
            continue
        if index + 1 >= len(headings):
            # Without a later boundary, finding prose cannot be separated from
            # an unmarked summary. Keep both in the collapsed review.
            continue
        end = headings[index + 1][0]
        if (
            unclosed_fence_start is not None
            and start < unclosed_fence_start < end
        ):
            # Once structure becomes ambiguous, preserving extra prose is safer than
            # deleting a genuine later finding or the summary.
            continue
        if any(start < offset < end for offset in hidden_finding_offsets):
            # A balanced fence can enclose a heading-like line that may instead
            # be a malformed finding boundary. Preserve the ambiguous range.
            continue
        ranges.append((start, end))

    for start, end in reversed(ranges):
        review = review[:start] + review[end:]
    return _remove_empty_finding_groups(review).strip()


def _remove_empty_finding_groups(review: str) -> str:
    """Remove finding-group headings that contain no remaining content."""
    boundaries = _review_boundaries(review)
    ranges: list[tuple[int, int]] = []
    for index, (start, line) in enumerate(boundaries):
        if _FINDING_GROUP_HEADING.match(line) is None:
            continue
        end = next(
            (
                boundary_start
                for boundary_start, boundary_line in boundaries[index + 1 :]
                if _SECTION_HEADING.match(boundary_line) is not None
            ),
            len(review),
        )
        if not review[start + len(line) : end].strip():
            ranges.append((start, end))

    for start, end in reversed(ranges):
        review = review[:start] + review[end:]
    return review


def _review_boundaries(review: str) -> list[tuple[int, str]]:
    """Return finding and section boundaries, ignoring balanced code fences."""
    return _parse_review_boundaries(review)[0]


def _parse_review_boundaries(
    review: str,
) -> tuple[list[tuple[int, str]], int | None, list[int]]:
    """Return reliable boundaries plus locations made ambiguous by fences."""
    headings: list[tuple[int, str]] = []
    hidden_finding_offsets: list[int] = []
    fence_character: str | None = None
    fence_length = 0
    fence_start: int | None = None
    offset = 0

    for line in review.splitlines(keepends=True):
        fence = _CODE_FENCE.match(line)
        if fence is not None:
            marker = fence.group(1)
            if fence_character is None:
                fence_character = marker[0]
                fence_length = len(marker)
                fence_start = offset
            elif marker[0] == fence_character and len(marker) >= fence_length:
                fence_character = None
                fence_length = 0
                fence_start = None
            offset += len(line)
            continue
        if _is_review_boundary(line):
            if fence_character is None:
                headings.append((offset, line))
            elif _FINDING_HEADING.match(line) is not None:
                hidden_finding_offsets.append(offset)
        offset += len(line)
    return headings, fence_start, hidden_finding_offsets


def _is_review_boundary(line: str) -> bool:
    return (
        _FINDING_HEADING.match(line) is not None
        or _SECTION_HEADING.match(line) is not None
    )


def should_skip_inline_review(
    payload: dict[str, Any],
    history: Any,
    trusted_bot_logins: Collection[str] = DEFAULT_TRUSTED_BOT_LOGINS,
) -> bool:
    """Return whether a comment-free inline payload repeats a prior review body."""
    comments = payload.get("comments")
    body = payload.get("body")
    return (
        comments == []
        and isinstance(body, str)
        and is_duplicate_review(body, history, trusted_bot_logins)
    )


def _diff_path(value: str) -> str | None:
    value = value.split("\t", 1)[0]
    if value == "/dev/null":
        return None
    if value.startswith(("a/", "b/")):
        value = value[2:]
    return value


def _inline_markers(marker: str) -> tuple[str, str]:
    return (
        f"<!-- AI_REVIEW_INLINE_START_{marker} -->",
        f"<!-- AI_REVIEW_INLINE_END_{marker} -->",
    )


def _validate_finding(finding: Any) -> tuple[str, str, int, str, str]:
    expected = set(INLINE_FINDING_FIELDS)
    if not isinstance(finding, dict) or set(finding) != expected:
        fields = ", ".join(INLINE_FINDING_FIELDS)
        raise ValueError(f"each inline finding must contain exactly: {fields}")

    finding_id = finding["id"]
    path = finding["path"]
    line = finding["line"]
    side = finding["side"]
    body = finding["body"]
    if not isinstance(finding_id, str) or _FINDING_ID.fullmatch(finding_id) is None:
        raise ValueError("inline finding ID must match Blocker1 or Nit1 form")
    if (
        not isinstance(path, str)
        or not path
        or len(path) > 500
        or path.startswith("/")
        or "\x00" in path
        or ".." in Path(path).parts
    ):
        raise ValueError(f"inline finding {finding_id} has an invalid path")
    if isinstance(line, bool) or not isinstance(line, int) or line < 1:
        raise ValueError(f"inline finding {finding_id} has an invalid line")
    if side not in INLINE_FINDING_SIDES:
        raise ValueError(f"inline finding {finding_id} has an invalid side")
    if not isinstance(body, str) or not body.strip() or len(body) > 10_000:
        raise ValueError(f"inline finding {finding_id} has an invalid body")
    if any(ord(char) < 32 and char not in "\n\t" for char in body):
        raise ValueError(f"inline finding {finding_id} has control characters")
    return finding_id, path, line, side, body.strip()


def _load_history(path: Path) -> Any:
    """Load review history, falling back to no history when it is unavailable."""
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def _load_trusted_bot_logins(path: Path) -> list[str]:
    """Load the workflow-owned bot allowlist, rejecting malformed input."""
    trusted_bot_logins = json.loads(path.read_text())
    if not isinstance(trusted_bot_logins, list) or not all(
        isinstance(login, str) for login in trusted_bot_logins
    ):
        raise ValueError("trusted bot logins must be a JSON array of strings")
    return trusted_bot_logins


def main() -> None:
    """Build a GitHub review request from workflow-owned files."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--review", required=True, type=Path)
    parser.add_argument("--findings", required=True, type=Path)
    parser.add_argument("--diff", required=True, type=Path)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--history", required=True, type=Path)
    parser.add_argument("--trusted-bot-logins", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--unmapped-output", required=True, type=Path)
    parser.add_argument("--duplicate-output", required=True, type=Path)
    parser.add_argument("--skip-duplicate-review-output", required=True, type=Path)
    args = parser.parse_args()

    history = _load_history(args.history)
    trusted_bot_logins = _load_trusted_bot_logins(args.trusted_bot_logins)
    payload, unmapped, duplicates = build_review_payload(
        review=args.review.read_text(),
        findings=json.loads(args.findings.read_text()),
        diff=args.diff.read_text(errors="replace"),
        head_sha=args.head_sha,
        run_url=args.run_url,
        history=history,
        trusted_bot_logins=trusted_bot_logins,
    )
    args.output.write_text(json.dumps(payload))
    args.unmapped_output.write_text("\n".join(unmapped) + ("\n" if unmapped else ""))
    args.duplicate_output.write_text(
        "\n".join(duplicates) + ("\n" if duplicates else "")
    )
    args.skip_duplicate_review_output.write_text(
        (
            "true\n"
            if should_skip_inline_review(payload, history, trusted_bot_logins)
            else "false\n"
        )
    )


if __name__ == "__main__":
    main()
