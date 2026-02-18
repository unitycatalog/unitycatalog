# Documentation for Unity Catalog

Unity Catalog uses [**Astro Starlight**](https://starlight.astro.build/) to manage and serve its documentation. Astro Starlight is a simple, static site generator that’s geared towards project documentation and is written in Node.js.

This guide will help you set up the local environment, serve the documentation locally, and deploy it to GitHub Pages.

## Setting Up for Local Development

To start testing or modifying the documentation locally, follow these steps:

### 1. Install Node.js

Install the required node version using [nvm](https://github.com/nvm-sh/nvm):

```
nvm install
```

Install dependencies:

```
pnpm i
```

### 2. Serving the Docs Locally

After installing all dependencies, you can now serve the documentation locally. Run the following command:

```sh
npm run dev
```

This will start a local development server. By default, the docs will be served at `http://127.0.0.1:4321/`. You can visit this link in your browser to see your documentation live as you edit it. Any changes made to the markdown files will automatically update the browser view.

### 3. Modifying the Documentation

You can make changes to the documentation by editing the markdown files under the `docs` directory. Astro will watch for changes and automatically refresh your browser.

- **Structure of the documentation** is in `docs/astro.config.mjs`. The file defines the site structure, theme, and various configurations.
- **Adding new pages**: Add new markdown files to the `docs/src/content/docs` folder and then update the `astro.config.mjs` file to include the new pages in the navigation.

> Note when **adding new a Integrations page**: Please include a reference to the new page in the `docs/src/content/docs/integrations/index.md` overview file to make it easier for users to find your integration from linked tutorials and blogs.

### 4. Testing Locally

After making changes to the documentation, be sure to view the changes at `http://127.0.0.1:4321/` to verify that everything looks and functions as expected.

---

## Deploying the Documentation

After you're satisfied with the changes, please deploy the documentation to your own repo's GitHub Pages. Please include a link to this deployment for any docs PRs, as this will help streamline reviews.

### 1. Ensure GitHub Pages is Enabled

Make sure GitHub Pages is enabled in your repository settings. To do this, go to your repository's **Settings** and scroll down to the **GitHub Pages** section. Choose the `gh-pages` branch as the source.

### 2. Deploy the Docs With Astro

Astro can automatically deploy the site to the `gh-pages` branch of your repository. To do so, simply run:

```sh
npm run dev -- --base=unitycatalog
```

This command will build the documentation to the `docs/dist` folder while still being publishable to your own Github pages.

Next, create a `gh-pages` branch which only includes the contents of the `docs/dist` folder. Once you push that up, then you can access the following URL:

```console
https://<your-github-username>.github.io/unitycatalog
```

For example, if your GitHub username is `bobbiedraper`, your documentation will be available at:

```console
https://bobbiedraper.github.io/unitycatalog
```

---

## Additional Resources

For more detailed instructions on using Astro or Astro Starlight, check out the official documentation:

- [Astro Starlight Documentation](https://starlight.astro.build/)
- [Astro Documentation](https://astro.build)

## Guidelines for Markdown formatting

This section mentions guidelines to follow for a proper formatting of Markdown in our documentation.

As general guideline we are using [Prettier](https://prettier.io/) to ensure a common formatting of Markdown files. Due to the usage of [Astro](https://astro.build/) we define a custom definition of rules. The definition is done in the `docs/.prettierrc` file.

Prettier can be ran manually by running:

```sh
npm run format
```

Alternatively `npm run format:fix` will attempt to fix files.
