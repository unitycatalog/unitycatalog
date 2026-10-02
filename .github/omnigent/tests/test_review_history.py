"""Tests for bounded previous-review selection and deduplication."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


def _load_module():
    path = Path(__file__).parents[1] / "review_history.py"
    spec = importlib.util.spec_from_file_location("review_history", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _document(
    *, author_type: str = "Bot", author_login: str = "github-actions"
) -> dict:
    marker = "<!-- ai-review-bot -->"
    return {
        "data": {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "nodes": [
                            {
                                "author": {
                                    "__typename": author_type,
                                    "login": author_login,
                                },
                                "body": f"{marker}\n## AI Review\n\nOld collapsed finding",
                                "createdAt": "2026-09-10T01:00:00Z",
                            }
                        ]
                    },
                    "reviews": {
                        "nodes": [
                            {
                                "author": {
                                    "__typename": author_type,
                                    "login": author_login,
                                },
                                "body": f"{marker}\n## AI Review\n\nOld review summary",
                                "submittedAt": "2026-09-10T02:00:00Z",
                                "comments": {
                                    "nodes": [
                                        {
                                            "path": "kernel/src/example.rs",
                                            "line": 10,
                                            "originalLine": 10,
                                            "fullDatabaseId": 101,
                                            "side": "RIGHT",
                                            "body": "**Blocker1** Repeated finding.",
                                        }
                                    ]
                                },
                            }
                        ]
                    },
                }
            }
        }
    }


class ReviewHistoryTest(unittest.TestCase):
    def test_history_includes_marked_bot_reviews_and_inline_comments(self) -> None:
        history = _load_module().format_review_history(_document())

        self.assertIn("untrusted historical data", history)
        self.assertIn("Old collapsed finding", history)
        self.assertIn("Old review summary", history)
        self.assertIn("Repeated finding", history)
        self.assertIn("kernel/src/example.rs", history)

    def test_history_ignores_spoofed_user_marker(self) -> None:
        module = _load_module()
        document = _document(author_type="User")

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_history_ignores_marker_from_untrusted_bot(self) -> None:
        module = _load_module()
        document = _document(author_login="untrusted-app")

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])
        self.assertFalse(module.is_duplicate_review("Old collapsed finding", document))

    def test_history_accepts_configured_bot_with_optional_suffix(self) -> None:
        module = _load_module()
        document = _document(author_login="omnigent-reviewer[bot]")
        trusted_logins = ["omnigent-reviewer"]

        self.assertIn(
            "Old collapsed finding",
            module.format_review_history(document, trusted_logins),
        )
        self.assertEqual(
            len(module.previous_inline_comments(document, trusted_logins)), 1
        )

    def test_inline_comments_are_available_for_exact_deduplication(self) -> None:
        comments = _load_module().previous_inline_comments(_document())

        self.assertEqual(
            comments,
            [
                {
                    "path": "kernel/src/example.rs",
                    "line": 10,
                    "side": "RIGHT",
                    "body": "**Blocker1** Repeated finding.",
                }
            ],
        )

    def test_inline_comments_fall_back_to_original_line(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment["line"] = None

        self.assertEqual(module.previous_inline_comments(document)[0]["line"], 10)

        comment["originalLine"] = None
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comment_sides_are_joined_from_rest_metadata(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment.pop("side")

        module.attach_inline_comment_sides(
            document,
            [{"id": 101, "side": None, "original_side": "LEFT"}],
        )

        self.assertEqual(module.previous_inline_comments(document)[0]["side"], "LEFT")

    def test_inline_dedup_fails_open_without_rest_side_metadata(self) -> None:
        module = _load_module()
        document = _document()
        comment = document["data"]["repository"]["pullRequest"]["reviews"]["nodes"][0][
            "comments"
        ]["nodes"][0]
        comment.pop("side")

        module.attach_inline_comment_sides(document, [])

        self.assertEqual(module.previous_inline_comments(document), [])

    def test_inline_comment_sides_ignore_malformed_rest_metadata(self) -> None:
        module = _load_module()
        malformed_metadata = (
            {},
            [{"id": "101", "side": "RIGHT"}],
            [{"id": 101, "side": "right"}],
        )

        for rest_comments in malformed_metadata:
            with self.subTest(rest_comments=rest_comments):
                document = _document()
                comment = document["data"]["repository"]["pullRequest"]["reviews"][
                    "nodes"
                ][0]["comments"]["nodes"][0]
                comment.pop("side")

                module.attach_inline_comment_sides(document, rest_comments)

                self.assertEqual(module.previous_inline_comments(document), [])

    def test_empty_history_document_is_supported(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.format_review_history({}),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments({}), [])
        self.assertFalse(module.is_duplicate_review("review", {}))

    def test_history_ignores_unmarked_bot_entries(self) -> None:
        module = _load_module()
        document = _document()
        pull_request = document["data"]["repository"]["pullRequest"]
        pull_request["comments"]["nodes"][0]["body"] = "Unmarked bot comment"
        pull_request["reviews"]["nodes"][0]["body"] = "Unmarked bot review"

        self.assertEqual(
            module.format_review_history(document),
            "No previous AI review findings were found.",
        )
        self.assertEqual(module.previous_inline_comments(document), [])

    def test_finding_normalization_ignores_run_specific_id_and_whitespace(self) -> None:
        module = _load_module()

        self.assertEqual(
            module.canonical_finding_body("**Nit2**  Repeated\n finding."),
            module.canonical_finding_body("**Blocker1** Repeated finding."),
        )

    def test_complete_review_deduplication_round_trips_publication_wrapper(self) -> None:
        module = _load_module()
        review_publish_path = Path(__file__).parents[1] / "review_publish.py"
        spec = importlib.util.spec_from_file_location(
            "review_publish", review_publish_path
        )
        assert spec is not None and spec.loader is not None
        review_publish = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(review_publish)
        document = _document()
        document["data"]["repository"]["pullRequest"]["comments"]["nodes"][0][
            "body"
        ] = review_publish.format_review_body(
            "Old collapsed finding",
            "https://github.com/delta-io/delta-kernel-rs/actions/runs/1",
            collapsed=True,
        )

        self.assertTrue(module.is_duplicate_review("Old collapsed finding", document))
        self.assertIn("Old collapsed finding", module.format_review_history(document))
        self.assertNotIn("<details>", module.format_review_history(document))
        self.assertFalse(module.is_duplicate_review("New finding", document))

    def test_history_is_newest_first_and_bounded(self) -> None:
        module = _load_module()
        document = _document()
        nodes = document["data"]["repository"]["pullRequest"]["comments"]["nodes"]
        nodes.clear()
        review_nodes = document["data"]["repository"]["pullRequest"]["reviews"][
            "nodes"
        ]
        review_nodes.clear()
        marker = module.BOT_MARKER
        for index in (0, 2, 3):
            nodes.append(
                {
                    "author": {
                        "__typename": "Bot",
                        "login": "github-actions",
                    },
                    "body": f"{marker}\nreview-{index}-" + "x" * 3_500,
                    "createdAt": f"2026-09-10T0{index}:00:00Z",
                }
            )
        review_nodes.append(
            {
                "author": {"__typename": "Bot", "login": "github-actions"},
                "body": f"{marker}\nreview-1-" + "x" * 3_500,
                "submittedAt": "2026-09-10T01:00:00Z",
                "comments": {"nodes": []},
            }
        )

        history = module.format_review_history(document)

        self.assertLessEqual(len(history), module.MAX_HISTORY_CHARS)
        self.assertLess(history.index("review-3"), history.index("review-2"))
        self.assertLess(history.index("review-2"), history.index("review-1"))
        self.assertNotIn("review-0", history)

    def test_history_truncates_each_entry(self) -> None:
        module = _load_module()
        document = _document()
        node = document["data"]["repository"]["pullRequest"]["comments"]["nodes"][0]
        node["body"] = module.BOT_MARKER + "\n" + "Z" * (module.MAX_ENTRY_CHARS + 100)

        history = module.format_review_history(document)

        self.assertEqual(history.count("Z"), module.MAX_ENTRY_CHARS)


if __name__ == "__main__":
    unittest.main()
