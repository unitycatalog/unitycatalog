# Users and Privileges

This page explains how Unity Catalog defines users and privileges.

Here's a quick overview:

- Unity Catalog stores information about users and privileges in a user database.
- Users can authenticate using an external authentication provider (e.g. Google Auth, Okta, etc.)
- After authentication, Unity Catalog will verify authorization to check whether a user has the required privileges to perform the requested action
- Admins can define users and privileges

## Enabling External Authentication

When you enable external authorization on a catalog, Unity Catalog automatically:

- creates an admin account in the user database
- creates an admin token
- grants the admin account the metastore admin privileges for the server.

Read the [Auth docs](auth.md) to learn how you can configure authentication and authorization.

## Default User Privileges

When you create a new catalog with external authentication enabled, Unity Catalog automatically creates an admin account. This admin account has full ownership over all catalogs, schemas and assets, as well as the metastore. They can use the admin token to create other users and give them privileges.

**Admins:**

- Use the admin token (in `etc/conf/token.txt`) to authenticate
- Have full privileges to access and transform catalogs, schemas and assets
- Can create user accounts

**Users:**

- Use Google Auth to generate authentication tokens
- Have limited privileges, as defined by admins
- Cannot create user accounts

## Creating New Users

Admins can create new users using the Unity Catalog CLI:

```sh
bin/uc --auth_token $(cat etc/conf/token.txt) user create --name "Bobbie Draper" --email bobbie@rocinante
```

This will add a new user to the local Unity Catalog user database.

## Granting User Privileges

An admin can grant users privileges using the Unity Catalog CLI.

For example, to grant a user the `USE CATALOG` privileg:

```sh
bin/uc --auth_token $(cat etc/conf/token.txt) permission create  --securable_type catalog --name unity --privilege 'USE CATALOG' --principal bobbie@rocinante
```

Or to grant the `CREATE CATALOG` privilege:

```sh
bin/uc --auth_token $(cat etc/conf/token.txt) permission create --securable_type metastore --name metastore --privilege "CREATE CATALOG" --principal bobbie@rocinante
```
