# Documentation for Unity Catalog

Unity Catalog uses **MkDocs** to manage and serve its documentation. MkDocs is a simple, static site generator that’s geared towards project documentation and is written in Python. This guide will help you set up the local environment, serve the documentation locally, and deploy it to GitHub Pages.

## Setting Up MkDocs for Local Development

To start testing or modifying the documentation locally, follow these steps:

### 1. Install MkDocs

Make sure you have **MkDocs** installed in your environment. If it’s not installed, you can do so using `pip`.

```bash
pip install mkdocs
```

You can verify that it is installed by running:

```bash
mkdocs --version
```

### 2. Clone the Repository

Clone the Unity Catalog repository and navigate to the documentation directory.

```bash
git clone https://github.com/unitycatalog/unitycatalog.git
cd unitycatalog/docs
```

### 3. Install the Required Dependencies

The `requirements-docs.txt` file contains all the dependencies necessary for serving the documentation, including **MkDocs** and its plugins. First, create a virtual environment:

```bash
# Create virtual environment
python -m venv uc_docs_venv

# Activate virtual environment (Linux/macOS)
source uc_docs_venv/bin/activate

# Activate virtual environment (Windows)
uc_docs_venv\Scripts\activate
```

Next, install the dependencies:

```bash
pip install -r requirements-docs.txt
```

### 4. Serving the Docs Locally

After installing all dependencies, you can now serve the documentation locally. Run the following command:

```bash
mkdocs serve
```

This will start a local development server. By default, the docs will be served at `http://127.0.0.1:8000/`. You can visit this link in your browser to see your documentation live as you edit it. Any changes made to the markdown files will automatically update the browser view.

### 5. Modifying the Documentation

You can make changes to the documentation by editing the markdown files under the `docs` directory. MkDocs will watch for changes and automatically refresh your browser.

- **Structure of the documentation** is controlled by the `mkdocs.yml` file located at the root of the repository. This file defines the site structure, theme, and various configurations.
  
- **Adding new pages**: Add new markdown files to the `docs` folder and then update the `mkdocs.yml` file to include the new pages in the navigation.

### 6. Testing Locally

After making changes to the documentation, be sure to view the changes at `http://127.0.0.1:8000/` to verify that everything looks and functions as expected.

---

## Deploying the Documentation

After you're satisfied with the changes, you can deploy the documentation to GitHub Pages.

### 1. Ensure GitHub Pages is Enabled

Make sure GitHub Pages is enabled in your repository settings. To do this, go to your repository's **Settings** and scroll down to the **GitHub Pages** section. Choose the `gh-pages` branch as the source.

### 2. Deploy the Docs with MkDocs

MkDocs can automatically deploy the site to the `gh-pages` branch of your repository. To do so, simply run:

```bash
mkdocs gh-deploy
```

This command will build the documentation and push the generated files to the `gh-pages` branch. Once the command is finished, your documentation will be live on GitHub Pages at:

```
https://<your-github-username>.github.io/unitycatalog
```

For example, if your GitHub username is `bobbiedraper`, your documentation will be available at:

```
https://bobbiedraper.github.io/unitycatalog
```

### 3. Updating Documentation

Each time you make updates to the documentation, you can run the `mkdocs gh-deploy` command to push the latest changes to GitHub Pages.

---

## Additional Resources

For more detailed instructions on using MkDocs, check out the official MkDocs documentation:

- [MkDocs Documentation](https://www.mkdocs.org/)
- [Deploying MkDocs to GitHub Pages](https://www.mkdocs.org/user-guide/deploying-your-docs/#github-pages)
