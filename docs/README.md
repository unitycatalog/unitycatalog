# README

## Serving the documentation with mkdocs

Create a virtual environment:

```sh
# Create virtual environment
python -m venv uc_docs_venv

# Activate virtual environment (Linux/macOS)
source uc_docs_venv/bin/activate

# Activate virtual environment (Windows)
uc_docs_venv\Scripts\activate
```

Install the required dependencies:

```sh
pip install -r requirements-docs.txt
```

Then serve the docs with

```sh
mkdocs serve
```
