# Unity Catalog UI

Unity Catalog UI is an intuitive user interface designed to manage and interact with Unity Catalog. It facilitates handling data permissions, auditing, and resource discovery in a user-friendly manner. Through this UI, users can efficiently view, create, update, and delete resources within the Unity Catalog server.

For more details on how to use the Unity Catalog UI, please refer to the [UI Documentation](https://github.com/unitycatalog/unitycatalog/tree/main/docs/ui).

![UC UI](../docs/assets/images/uc-ui.png)

# Prerequisite

Node: https://nodejs.org/en/download/package-manager

Bun: https://bun.com/docs/installation

Bun is the package manager and script runner for this project. The app itself is still built and served by
`react-scripts` running on Node.

## Get started

Spin up a localhost Unity Catalog server (e.g., `./bin/start-uc-server`), see https://github.com/unitycatalog/unitycatalog/blob/main/README.md#run-the-uc-server

Then in the project directory, you can run:

### `bun install`

Install all the necessary dependencies

### `bun run start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### Other scripts

- `bun run build` - build the production bundle into `build/`
- `bun run test` - run the test suite
- `bun run format` - apply Prettier and ESLint fixes
- `bun run test:format` - check formatting without writing changes (this is what CI runs)
- `bun run generate` - regenerate the API types from the OpenAPI specs in `../api`

Always invoke these through `bun run`. `build` and `test` are Bun's own subcommands, so `bun build` and `bun test`
would run Bun's bundler and test runner instead of the scripts above.

### Authenticate and Login

OSS Unity Catalog supports Sign in with Google. You can authenticate with Google by clicking the "Sign in with Google" button on the login page, once OAuth has been configured. To configure this, follow the steps to obtain a [Google API Client ID](https://developers.google.com/identity/gsi/web/guides/get-google-api-clientid) and configure your OAuth consent screen.

NOTE: The google client ID should match what is configured in the server.properties file on the server side. See README in root directory. In order for login to work, authentication must be enabled on server side AND UI side and users must be added to users table.

Once you have the client ID, add it to the `.env` file after `REACT_APP_GOOGLE_CLIENT_ID=` and change the `REACT_APP_GOOGLE_AUTH_ENABLED` flag from false to true. Restart the dev server. 

## References

This project has been merged into the main Unity Catalog repository. Per [Merging unitycatalog-ui repo into unitycatalog (main) repo (#349)](https://github.com/unitycatalog/unitycatalog/discussions/349).