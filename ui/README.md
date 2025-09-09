# Unity Catalog UI

Unity Catalog UI is an intuitive user interface designed to manage and interact with Unity Catalog. It facilitates handling data permissions, auditing, and resource discovery in a user-friendly manner. Through this UI, users can efficiently view, create, update, and delete resources within the Unity Catalog server.

For more details on how to use the Unity Catalog UI, please refer to the [UI Documentation](https://github.com/unitycatalog/unitycatalog/tree/main/docs/ui).

![UC UI](../docs/assets/images/uc-ui.png)

# Prerequisite

Node: https://nodejs.org/en/download/package-manager

Yarn: https://classic.yarnpkg.com/lang/en/docs/install

## Get started

Spin up a localhost Unity Catalog server (e.g., `./bin/start-uc-server`), see https://github.com/unitycatalog/unitycatalog/blob/main/README.md#run-the-uc-server

Then in the project directory, you can run:

### `yarn`

Install all the necessary dependencies

### `yarn start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### Authenticate and Login

Unity Catalog UI supports multiple authentication providers: Google OAuth, Microsoft Azure AD, Okta, and Keycloak. 

#### Configuration

1. **Copy the environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Configure your authentication provider in `.env`:**

   **For Google OAuth:**
   - Set `REACT_APP_GOOGLE_AUTH_ENABLED=true`
   - Add your Google client ID to `REACT_APP_GOOGLE_CLIENT_ID`
   - Follow [Google API Client ID setup](https://developers.google.com/identity/gsi/web/guides/get-google-api-clientid)

   **For Microsoft Azure AD:**
   - Set `REACT_APP_MS_AUTH_ENABLED=true` 
   - Add your Azure SPA client ID to `REACT_APP_MS_CLIENT_ID`
   - Add your tenant ID to `REACT_APP_MS_AUTHORITY=https://login.microsoftonline.com/your-tenant-id`

   **For Okta:**
   - Set `REACT_APP_OKTA_AUTH_ENABLED=true`
   - Configure `REACT_APP_OKTA_DOMAIN`, `REACT_APP_OKTA_CLIENT_ID`

   **For Keycloak:**
   - Set `REACT_APP_KEYCLOAK_AUTH_ENABLED=true`
   - Configure `REACT_APP_KEYCLOAK_URL`, `REACT_APP_KEYCLOAK_REALM_ID`, `REACT_APP_KEYCLOAK_CLIENT_ID`

3. **Server-side configuration:** The authentication provider must also be configured in the server's `etc/conf/server.properties`. See the main README for server setup details.

4. **Restart the UI:** Run `yarn start` after configuration changes.

**Important:** The `.env` file contains sensitive configuration and is not tracked by git. Never commit real credentials to the repository. 

## References

This project has been merged into the main Unity Catalog repository. Per [Merging unitycatalog-ui repo into unitycatalog (main) repo (#349)](https://github.com/unitycatalog/unitycatalog/discussions/349).