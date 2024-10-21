import pytest

from unitycatalog.ai.core.utils.docstring_utils import DocstringInfo, parse_docstring


def test_parse_docstring_empty():
    with pytest.raises(ValueError, match="Docstring is empty"):
        parse_docstring("")


def test_parse_docstring_missing_description():
    docstring = """
    Args:
        x: The input value.
    Returns:
        int: The input value incremented by one.
    """
    with pytest.raises(ValueError, match="Function description is missing"):
        parse_docstring(docstring)


def test_parse_docstring_single_line_single_param():
    docstring = """
    Add one to the input.

    Args:
        x: The input value.
    Returns:
        int: The input value incremented by one.
    """
    expected = DocstringInfo(
        description="Add one to the input.",
        params={"x": "The input value."},
        returns="int: The input value incremented by one.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_multiple_params():
    docstring = """
    Calculate the area of a rectangle.

    Args:
        width: The width of the rectangle.
        height: The height of the rectangle.
    Returns:
        float: The area of the rectangle.
    """
    expected = DocstringInfo(
        description="Calculate the area of a rectangle.",
        params={"width": "The width of the rectangle.", "height": "The height of the rectangle."},
        returns="float: The area of the rectangle.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_multi_line_param_description_with_colon():
    docstring = """
    Add one to the input.

    Args:
        x: The input value.
            For example: 4
    Returns:
        int: The input value incremented by one.
    """
    expected = DocstringInfo(
        description="Add one to the input.",
        params={"x": "The input value. For example: 4"},
        returns="int: The input value incremented by one.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_no_params():
    docstring = """
    Get the current timestamp.

    Returns:
        str: The current timestamp in ISO format.
    """
    expected = DocstringInfo(
        description="Get the current timestamp.",
        params={},
        returns="str: The current timestamp in ISO format.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_google_style():
    docstring = """
    Calculate the sum of two numbers.

    Args:
        a (int): The first number.
        b (int): The second number.

    Returns:
        int: The sum of a and b.
    """
    expected = DocstringInfo(
        description="Calculate the sum of two numbers.",
        params={"a": "The first number.", "b": "The second number."},
        returns="int: The sum of a and b.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_malformed_missing_sections():
    docstring = """
    Just a simple description without parameters or return information.
    """
    expected = DocstringInfo(
        description="Just a simple description without parameters or return information.",
        params={},
        returns=None,
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_parameters_without_descriptions():
    docstring = """
    Process data.

    Args:
        data:
        config:
    Returns:
        bool: Success status.
    """
    expected = DocstringInfo(
        description="Process data.",
        params={"data": None, "config": None},
        returns="bool: Success status.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_returns_without_description():
    docstring = """
    Check if the user is active.

    Args:
        user_id: The ID of the user.

    Returns:
        bool:
    """
    expected = DocstringInfo(
        description="Check if the user is active.",
        params={"user_id": "The ID of the user."},
        returns="bool:",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_extra_colons_in_description():
    docstring = """
    Add one to the input.

    Args:
        x: The input value.
            Note: This should be an integer.
    Returns:
        int: The input value incremented by one.
    """
    expected = DocstringInfo(
        description="Add one to the input.",
        params={"x": "The input value. Note: This should be an integer."},
        returns="int: The input value incremented by one.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_multiple_colons_in_description():
    docstring = """
    Configure the server.

    Args:
        config: Server configuration.
            Details: Should include IP, port, and protocol.
            Example: ip=127.0.0.1, port=8080, protocol=http
    Returns:
        None:
    """
    expected = DocstringInfo(
        description="Configure the server.",
        params={
            "config": "Server configuration. Details: Should include IP, port, and protocol. Example: ip=127.0.0.1, port=8080, protocol=http"
        },
        returns="None:",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_improper_newline_formatting():
    docstring = """
    Concat some values.

    Args:

        x: The input value.

        y: Another input value.



        z: Another input value.

    Returns:
        str: The input values concatenated.
    
    """
    expected = DocstringInfo(
        description="Concat some values.",
        params={
            "x": "The input value.",
            "y": "Another input value.",
            "z": "Another input value.",
        },
        returns="str: The input values concatenated.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_generic_placeholder():
    docstring = """
    Concat some values.
    Args:
        x: The input value.
        y: Another input value.
        z: ...
    Returns: ...
    """
    expected = DocstringInfo(
        description="Concat some values.",
        params={
            "x": "The input value.",
            "y": "Another input value.",
            "z": "...",
        },
        returns="...",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_multiline_description():
    docstring = """


    Concat some values.

    
    This function concatenates the input values.


    And does some other things.
    Args:
        x: The input value.
        y: Another input value.
    Returns:
        str: The input values concatenated.
    """
    expected = DocstringInfo(
        description="Concat some values. This function concatenates the input values. And does some other things.",
        params={
            "x": "The input value.",
            "y": "Another input value.",
        },
        returns="str: The input values concatenated.",
    )
    result = parse_docstring(docstring)
    assert result == expected


def test_parse_docstring_single_line_invalid_structured_comments():
    docstring = """
    Concat some values.
    
    Args: x: The input value.
    Returns: str: The input values concatenated.
    """
    expected = DocstringInfo(
        description="Concat some values.",
        params={
            "x": "The input value.",
        },
        returns="str: The input values concatenated.",
    )
    result = parse_docstring(docstring)
    assert result == expected
