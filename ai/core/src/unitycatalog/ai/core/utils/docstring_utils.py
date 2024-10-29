from dataclasses import dataclass
from enum import Enum
from typing import Optional

PLACEHOLDER = "..."


@dataclass
class DocstringInfo:
    """Dataclass to store parsed docstring information."""

    description: str
    params: Optional[dict[str, Optional[str]]]
    returns: Optional[str]


class State(Enum):
    DESCRIPTION = "DESCRIPTION"
    ARGS = "ARGS"
    RETURNS = "RETURNS"
    END = "END"


def parse_docstring(docstring: str) -> DocstringInfo:
    """
    Parses the docstring to extract the function description, parameter comments,
    and return value description.
    Handles both reStructuredText and Google-style docstrings.
    Args:
        docstring: The docstring to parse.
    Returns:
        DocstringInfo: A dataclass containing the parsed information.
    Raises:
        ValueError: If the docstring is empty or missing a function description.
    """

    if not docstring or not docstring.strip():
        raise ValueError(
            "Docstring is empty. Please provide a docstring with a function description."
        )

    sections = _split_docstring_sections(docstring)
    description = _parse_description(sections["description"])
    params = _parse_params(sections["args"])
    returns = _parse_returns(sections["returns"])

    return DocstringInfo(description=description, params=params, returns=returns)


def _split_docstring_sections(docstring: str) -> dict[str, list[str]]:
    """Splits the docstring into sections: description, args, and returns."""
    lines = docstring.strip().splitlines()
    sections = {"description": [], "args": [], "returns": []}
    current_section = "description"
    section_titles = {"Args:": "args", "Arguments:": "args", "Returns:": "returns"}

    for line in lines:
        stripped_line = line.strip()
        matched_section = False
        for title, section in section_titles.items():
            if stripped_line.startswith(title):
                current_section = section
                content = stripped_line[len(title) :].strip()
                if content:
                    sections[current_section].append(content)
                matched_section = True
                break
        if matched_section:
            continue
        sections[current_section].append(line)
    return sections


def _parse_description(lines: list[str]) -> str:
    description_lines = [line.strip() for line in lines if line.strip()]
    description = " ".join(description_lines).strip()
    if not description:
        raise ValueError(
            "Function description is missing in the docstring. Please provide a function description."
        )
    return description


def _parse_params(lines: list[str]) -> dict[str, Optional[str]]:
    parsed_params = {}
    if not lines:
        return parsed_params

    param_indent = None
    current_param = None
    param_description_lines = []

    for line in lines:
        if not line.strip():
            continue

        stripped_line = line.lstrip()
        indent = len(line) - len(stripped_line)

        if param_indent is None:
            param_indent = indent

        if indent < param_indent:
            current_param, param_description_lines = _finalize_current_param(
                parsed_params, current_param, param_description_lines
            )
            continue

        if ":" in stripped_line:
            param_part, desc_part = stripped_line.split(":", 1)
            param_name = _extract_param_name(param_part.strip())

            if indent == param_indent:
                current_param, param_description_lines = _finalize_current_param(
                    parsed_params, current_param, param_description_lines
                )
                current_param = param_name
                param_description_lines = [desc_part.strip()] if desc_part.strip() else []
            else:
                if current_param:
                    param_description_lines.append(stripped_line)
        else:
            if current_param:
                param_description_lines.append(stripped_line)

    _finalize_current_param(parsed_params, current_param, param_description_lines)
    return parsed_params


def _parse_returns(lines: list[str]) -> Optional[str]:
    if not lines:
        return None
    return_description = " ".join(line.strip() for line in lines if line.strip()).strip()
    if return_description == PLACEHOLDER:
        return PLACEHOLDER
    return return_description if return_description else None


def _finalize_current_param(
    parsed_params: dict, current_param: Optional[str], param_description_lines: list[str]
) -> tuple[Optional[str], list[str]]:
    if current_param is not None:
        description = " ".join(param_description_lines).strip()
        parsed_params[current_param] = description if description else None
    return None, []


def _extract_param_name(param_part: str) -> str:
    if "(" in param_part and param_part.endswith(")"):
        return param_part.split("(", 1)[0].strip()
    return param_part
