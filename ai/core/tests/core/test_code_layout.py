import pytest

from unitycatalog.ai.core.utils.code_layout import (
    classify_lines,
    dedent_code,
    shift_stored_body,
    to_databricks_body,
    to_stored_body,
)


@pytest.mark.parametrize(
    ("code", "expected_string_lines", "expected_statement_lines"),
    [
        ("\\\n    if True:\n        return 1", set(), {1, 2}),
        ("  \\\n    x = 1\n  return x", set(), {0, 2}),
        ("# note\n\n\\\n    x = 1\nreturn x", set(), {3, 4}),
        ("x = (1,\n  2)\n# note\nreturn x", set(), {0, 3}),
        ('x = """a\n  b\n"""\nreturn x', {1, 2}, {0, 3}),
        ('"""A docstring\non two lines."""\nreturn 1', {1}, {0, 2}),
        ('x = f"""a{\n1}\nb"""\nreturn x', {1, 2}, {0, 3}),
        ('x = f"""{f"y"}\n  b"""\nreturn x', {1}, {0, 2}),
        ('x = """a\r\nb"""\r\nreturn x', {1}, {0, 2}),
        ('x = (1,\n    2\nreturn """a\n  b', set(), {0, 1, 2, 3}),
        ("x = 1\n\\", set(), {0, 1}),
    ],
    ids=[
        "column-0-backslash-start",
        "indented-backslash-start",
        "column-0-backslash-start-after-comment-and-blank",
        "bracket-continuation",
        "string",
        "docstring",
        "multiline-replacement-field",
        "nested-fstring",
        "crlf",
        "does-not-tokenize",
        "trailing-backslash",
    ],
)
def test_classify_lines(code, expected_string_lines, expected_statement_lines):
    assert classify_lines(code) == (expected_string_lines, expected_statement_lines)


@pytest.mark.parametrize(
    ("code", "expected_statement_lines"),
    [
        ("\\\n    if True:\n    return 1", {0, 2}),
        ("# note\n\n\\\n    x = 1\nreturn x", {2, 4}),
    ],
    ids=["column-0-backslash-start", "column-0-backslash-start-after-comment-and-blank"],
)
def test_classify_lines_read_with_margin_counts_backslash_lines_as_indented(
    code, expected_statement_lines
):
    assert classify_lines(code, read_with_margin=True) == (set(), expected_statement_lines)


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (
            "    if y:\n\\\n        return y\n\\\n    return 2",
            "if y:\n    \\\n    return y\n\\\nreturn 2",
        ),
        ("    s = '''a\n\\\nb'''\n    return s", "s = '''a\n\\\nb'''\nreturn s"),
    ],
    ids=["code", "string"],
)
def test_dedent_code_aligns_column_0_backslash_lines_with_the_next_code_line(code, expected):
    assert dedent_code(code) == expected


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (
            'x = """a\n b\n  c"""\nreturn x',
            '    x = """a\n b\n  c"""\n    return x',
        ),
        (
            'if x:\n  y = """a\n   b"""\nreturn y',
            '    if x:\n      y = """a\n   b"""\n    return y',
        ),
        (
            'x = """a\n\n      b"""\nreturn x',
            '    x = """a\n\n      b"""\n    return x',
        ),
        (
            '\\\nx = """a\nb"""\nreturn x',
            '    \\\n    x = """a\nb"""\n    return x',
        ),
    ],
    ids=["lines-at-1-and-2", "nested-block", "blank-line", "backslash-start"],
)
@pytest.mark.parametrize(
    "write",
    [to_stored_body, lambda body: to_databricks_body(body, indent_unit=2)],
    ids=["oss", "databricks"],
)
def test_written_body_reads_back_with_strings_as_written(write, body, expected):
    assert shift_stored_body(write(body)) == expected
