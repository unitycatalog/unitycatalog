# WARNING: Archive repository in process
Per [Merging unitycatalog-ui repo into unitycatalog (main) repo (#349)](https://github.com/unitycatalog/unitycatalog/discussions/349), as of September 2nd, 2024, we have merged the `unitycatalog-ui/main` repo/branch into `unitycatalog/ui`. Upon debug/review, we will archive this repository on September 5th, 2024.

# Prerequisite

Node: https://nodejs.org/en/download/package-manager

Yarn: https://classic.yarnpkg.com/lang/en/docs/install

## Get started

Spin up a localhost Unity Catalog server, see https://github.com/unitycatalog/unitycatalog/blob/main/README.md#run-the-uc-server

Then in the project directory, you can run:

### `yarn`

Install all the necessary dependencies

### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.


### Authenticate and Login

OSS Unity Catalog supports Sign in with Google. You can authenticate with Google by clicking the "Sign in with Google" button on the login page, once OAuth has been configured. To configure this, follow the steps to obtain a [Google API Client ID](https://developers.google.com/identity/gsi/web/guides/get-google-api-clientid) and configure your OAuth consent screen.

Once you have the client ID, add it to the `.env` file after `REACT_APP_GOOGLE_CLIENT_ID=` and change the `REACT_APP_GOOGLE_AUTH_ENABLED` flag from false to true. Restart yarn. 
