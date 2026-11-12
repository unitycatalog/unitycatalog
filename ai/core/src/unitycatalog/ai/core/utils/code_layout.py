"""
Re-indentation of Python function bodies that keeps multi-line string literals intact.

The leading whitespace of a line inside a multi-line string literal is part of the string's
value, so these helpers never treat it as indentation.

Unity Catalog stores a Python function as its body, without the `def` line. The OSS layout is
the one a top-level function with 4-space indentation gets when its body is dedented as a
whole: statements start at column `4 - k`, and the lines of multi-line string literals sit `k`
columns left of where they were written, where `k` is at most 4 and at most the leading spaces
of any nonempty string line. Empty string lines stay empty, as they do under `textwrap.dedent`.
A Databricks body has the same relation between code and string lines, indented further. A
reader puts the statements at `PRIMARY_INDENT` and moves the nonempty string lines by the same
number of columns, which restores their values for every body in that layout.
"""

import io
import os
import tokenize

PRIMARY_INDENT = " " * 4

_STRING_START_TOKENS = {
    getattr(tokenize, name)
    for name in ("FSTRING_START", "TSTRING_START")
    if hasattr(tokenize, name)
}
_STRING_END_TOKENS = {
    getattr(tokenize, name) for name in ("FSTRING_END", "TSTRING_END") if hasattr(tokenize, name)
}
_NON_STATEMENT_TOKENS = {
    tokenize.NL,
    tokenize.NEWLINE,
    tokenize.COMMENT,
    tokenize.INDENT,
    tokenize.DEDENT,
    tokenize.ENDMARKER,
}


def classify_lines(code: str) -> tuple[set[int], set[int]]:
    """
    Return the 0-based indices of the lines that continue a string literal opened on an
    earlier line, and of the lines that start a statement.

    Only statement lines carry indentation that Python checks. Comment lines and lines that
    continue a statement (inside brackets or after a backslash) may sit at any column, so they
    must not decide the margin. A statement that is only a string, such as a docstring, is not
    reported as string lines: its value is never used by the code, and `inspect.cleandoc`
    removes its indentation, so it is laid out like the code around it. Code that does not
    tokenize cannot compile either, so it is treated as having no string lines and every
    nonblank line as a statement line.
    """
    string_lines = set()
    statement_lines = set()
    open_string_lines = []
    at_statement_start = True
    string_statement_lines = range(0)
    try:
        for token in tokenize.generate_tokens(io.StringIO(code).readline):
            if string_statement_lines and token.type not in (tokenize.NL, tokenize.COMMENT):
                if token.type != tokenize.NEWLINE:
                    string_lines.update(string_statement_lines)
                string_statement_lines = range(0)
            starts_statement = at_statement_start and token.type not in _NON_STATEMENT_TOKENS
            if token.type == tokenize.NEWLINE:
                at_statement_start = True
            elif starts_statement:
                statement_lines.add(token.start[0] - 1)
                at_statement_start = False
            if token.type in _STRING_START_TOKENS:
                open_string_lines.append(token.start[0])
            elif token.type in _STRING_END_TOKENS:
                string_lines.update(range(open_string_lines.pop(), token.end[0]))
            elif token.type == tokenize.STRING and starts_statement:
                string_statement_lines = range(token.start[0], token.end[0])
            elif token.type == tokenize.STRING:
                string_lines.update(range(token.start[0], token.end[0]))
    except (tokenize.TokenError, SyntaxError):
        return set(), {index for index, line in enumerate(code.split("\n")) if line.strip()}
    return string_lines, statement_lines


def _leading_whitespace(line: str) -> str:
    return line[: len(line) - len(line.lstrip())]


def _leading_spaces(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_empty(line: str) -> bool:
    """Whether a line has no characters before its line break, which may be `\\r\\n`."""
    return line in ("", "\r")


def _statement_margin(lines: list[str], statement_lines: set[int]) -> str:
    return os.path.commonprefix([_leading_whitespace(lines[index]) for index in statement_lines])


def _strip_margin(line: str, margin: str) -> str:
    """Remove `margin` from a code line, or as much of it as the line has."""
    return line[len(os.path.commonprefix([_leading_whitespace(line), margin])) :]


def dedent_code(code: str) -> str:
    """
    Like `textwrap.dedent`, but the margin comes from statement lines only, string literal
    lines are left untouched, and comment or continuation lines that sit left of the margin
    move only as far as column 0.
    """
    string_lines, statement_lines = classify_lines(code)
    lines = code.split("\n")
    margin = _statement_margin(lines, statement_lines)
    return "\n".join(
        line if index in string_lines else _strip_margin(line, margin) if line.strip() else ""
        for index, line in enumerate(lines)
    )


def to_stored_body(body: str) -> str:
    """
    Convert a body from `dedent_code`, whose statements start at column 0 and whose string
    lines sit where they were written, to the OSS layout described in the module docstring.
    """
    string_lines, _ = classify_lines(body)
    lines = body.split("\n")
    shift = min(
        [len(PRIMARY_INDENT)]
        + [_leading_spaces(lines[index]) for index in string_lines if not _is_empty(lines[index])]
    )
    code_indent = PRIMARY_INDENT[shift:]
    return "\n".join(
        line[shift:] if index in string_lines else code_indent + line if line.strip() else ""
        for index, line in enumerate(lines)
    )


def to_databricks_body(body: str, indent_unit: int) -> str:
    """
    Indent a body from `dedent_code` for the `AS $$ ... $$` block of a Databricks function.

    Databricks runs this text, so string lines stay where they were written, and only the code
    is indented. A body with multi-line strings is indented by `PRIMARY_INDENT`, the margin at
    which `shift_stored_body` reads those string lines back unchanged. Any other body keeps the
    indentation unit of the function's source.
    """
    string_lines, _ = classify_lines(body)
    indent = PRIMARY_INDENT if string_lines else " " * indent_unit
    return "\n".join(
        indent + line if index not in string_lines and line.strip() else line
        for index, line in enumerate(body.split("\n"))
    )


def shift_stored_body(body: str) -> str:
    """
    Move a stored body so that its statements start at `PRIMARY_INDENT`, and move its string
    lines by the same number of columns, never past their own leading spaces. This restores
    string values for OSS and Databricks bodies alike.
    """
    string_lines, statement_lines = classify_lines(body)
    lines = body.split("\n")
    margin = _statement_margin(lines, statement_lines)
    shift = len(PRIMARY_INDENT) - len(margin)

    def shift_string_line(line: str) -> str:
        if _is_empty(line):
            return line
        if shift >= 0:
            return " " * shift + line
        return line[min(-shift, _leading_spaces(line)) :]

    return "\n".join(
        shift_string_line(line)
        if index in string_lines
        else PRIMARY_INDENT + _strip_margin(line, margin)
        if line.strip()
        else ""
        for index, line in enumerate(lines)
    )
