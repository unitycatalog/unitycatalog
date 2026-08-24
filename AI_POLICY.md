# AI-Assisted Contribution Policy

Unity Catalog welcomes contributions made with the help of AI coding assistants. These tools can help you explore the codebase, draft changes, write tests, and review pull requests. This policy sets the ground rules so that AI use strengthens the project without lowering the bar for quality, correctness, or review. It complements `CONTRIBUTING.md` (how to contribute) and `AGENTS.md` (how the code is built, tested, and structured).

## The core principle: you are accountable

AI is a tool, not a co-author that shares responsibility. Whatever an assistant produces, you own it. You must understand it, be able to explain and defend it in review, and stand behind its correctness and its licensing. If you cannot do that for a change, it is not ready to submit.

## Using AI to write code

- **Understand every line.** Do not open a pull request containing code you cannot explain. The most common review question on this project is "why?", and you are expected to answer it for any line in your diff.
- **Validate before you submit.** Build, test, and self-review AI-produced changes as if you had written them by hand (see `AGENTS.md`). Do not submit raw, unverified output.
- **Make the pull request yours.** Write the title and description in your own words, explaining the motivation and the change. Do not paste an assistant's summary you have not checked against the actual diff.
- **Keep it scoped and lean.** AI tends to over-produce. Trim speculative abstractions, redundant tests, and boilerplate, and keep pull requests small and focused, as `CONTRIBUTING.md` asks.
- **Licensing and attribution are your responsibility.** Ensure AI-generated content does not include unattributed third-party or copyrighted material. Your Developer Certificate of Origin sign-off (see `CONTRIBUTING.md`) certifies that you have the right to contribute the change.

## Using AI to review code

- **The human reviewer is accountable for every comment posted.** You may use AI to help you review, but verify each finding against the actual code before posting, and do not post speculative or low-confidence AI output. Prefer no comment over a weak one.
- **Approve each comment individually; nothing is auto-posted.** Triggering an AI review, even one you asked for, is not approval to post its output. AI findings go to a draft or the reviewer's own notes first, and a human decides, comment by comment, what actually lands on the pull request. Never wire automation to post AI findings straight to a PR.
- **Disclose AI assistance.** When a review comment is substantially AI-generated, say so in the pull request or review thread; marking such comments (for example, "(ai assisted)") is encouraged.
- **Approval and merge are human decisions.** An assistant may summarize a change or suggest issues, but a maintainer decides whether to approve and merge.

## Not acceptable

- Raw, unreviewed AI-generated pull requests opened without genuine author understanding or engagement.
- Unedited AI-generated review comments passed off as the reviewer's own considered judgment.
- Bots or automation posting AI-generated content to issues or pull requests without human approval.

## Why this matters

Unity Catalog governs access to data: catalogs, schemas, tables, volumes, functions, models, credentials, and the privileges over them. A subtle bug or an authorization mistake here has direct consequences for the data a deployment protects. Effective review depends on authors understanding their own changes well enough to engage substantively with feedback, so a high bar for understanding is not optional.

## New to the project?

If you are still learning the codebase, the most valuable first contributions are high-quality issue reports and small, well-tested fixes, rather than large AI-generated changes. Start with `CONTRIBUTING.md`, and see `AGENTS.md` for how the project is built, tested, and organized.
