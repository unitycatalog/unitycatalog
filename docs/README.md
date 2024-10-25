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

## Guidelines for Markdown formatting

This section mentions guidelines to follow for a proper formatting of Markdown in our documentation.

### Formatting code snippets within a list

If code snippets are present within a list, they should be aligned with the content of the list. The following section
shows an example of a proper formatting:

- The tarball generated in the `target` directory can be unpacked using the following command:

    ```sh
    tar -xvf unitycatalog-<version>.tar.gz
    ```

- Unpacking the tarball will create the following directory structure:

    ```console
    unitycatalog-<version>
    ├── bin
    │   ├── start-uc-server
    │   └── uc
    ├── etc
    │   ├── conf
    │   ├── data
    │   ├── db
    │   └── logs
    └── jars
    ```

Please note that this ensures that the code snippet is aligned with the text in the bullet points. The final result
should look similar to the following

![Markdown code snippet alignment in a list](./assets/images/markdown-code-snippet-list-aligned.png)

In comparison an invalid alignment looks like this

![Markdown code snippet wrong alignment in a list](./assets/images/markdown-code-snippet-list-unaligned.png)
