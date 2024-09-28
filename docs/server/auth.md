# Unity Catalog Authentication and Authorization

You can start working with Unity Catalog Access Control integration with an external authentication provider (e.g., Google Auth, Okta, etc.) via the following instructions.  

It is important to note that Unity Catalog already has its own local user database to restrict user access.  When you work with an external authentication provider, we are now relying on that external provider (e.g., Google Identity) to authenticate. For example, for access to Unity Catalog authenticated by Google Identity, a user must have a Google Identity (e.g. gmail address) that is added to the local UC database.   

Throughout the next set of examples, we are using Google Identity for authentication while the local Unity Catalog database for authorization.  The permissions (e.g., `USE CATALOG`, `SELECT`, etc.) are the authorized set of tasks the user account can perform.  The user account and what tasks they are authorized to do are stored in your local database.  

