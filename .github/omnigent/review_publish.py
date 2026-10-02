"""Format and unwrap validated AI review text for GitHub publication."""

from __future__ import annotations

import re


BOT_MARKER = "<!-- ai-review-bot -->"
REVIEW_HEADER = "## AI Review <sub>(draft - human review required)</sub>"
_REVIEW_FOOTER = re.compile(
    r"\n+---\n+<sub>Automated review - \[workflow run\]\([^\n]+\)</sub>\s*$"
)
_PUBLICATION_MARKER = re.compile(r"[0-9a-f]{32}")


def extract_marked_review(raw: str, marker: str) -> str:
    """Return the final complete review delimited by a per-run marker pair."""
    if _PUBLICATION_MARKER.fullmatch(marker) is None:
        raise ValueError("publication marker is invalid")

    start = f"<!-- AI_REVIEW_START_{marker} -->"
    end = f"<!-- AI_REVIEW_END_{marker} -->"
    token_pattern = re.compile(f"({re.escape(start)}|{re.escape(end)})")
    current_start: int | None = None
    reviews: list[str] = []
    for token in token_pattern.finditer(raw):
        if token.group(0) == start:
            if current_start is not None:
                raise ValueError("publication markers are nested")
            current_start = token.end()
            continue
        if current_start is None:
            raise ValueError("publication end marker has no matching start marker")
        reviews.append(raw[current_start : token.start()].strip())
        current_start = None

    if current_start is not None:
        raise ValueError("publication start marker has no matching end marker")
    if not reviews:
        raise ValueError("publication markers are missing")
    if not reviews[-1]:
        raise ValueError("final marked review is empty")
    return reviews[-1]


def format_review_body(review: str, run_url: str, *, collapsed: bool) -> str:
    """Add the shared header and footer to a publishable review."""
    if not run_url.startswith("https://github.com/"):
        raise ValueError("run URL must be a GitHub HTTPS URL")

    body = review.strip()
    if collapsed:
        body = f"<details><summary>Show review</summary>\n\n{body}\n\n</details>"
    return (
        f"{BOT_MARKER}\n"
        f"{REVIEW_HEADER}\n\n"
        f"{body}\n\n"
        "---\n"
        f"<sub>Automated review - [workflow run]({run_url})</sub>"
    )


def strip_review_body(body: str) -> str:
    """Remove the wrapper added by :func:`format_review_body`."""
    body = body.strip()
    if body.startswith(BOT_MARKER):
        body = body[len(BOT_MARKER) :].lstrip()
    if body.startswith(REVIEW_HEADER):
        body = body[len(REVIEW_HEADER) :].lstrip()
    body = _REVIEW_FOOTER.sub("", body)
    details_start = "<details><summary>Show review</summary>"
    if body.startswith(details_start) and body.rstrip().endswith("</details>"):
        body = body[len(details_start) :].lstrip()
        body = body.rstrip()[: -len("</details>")].rstrip()
    return body.strip()
