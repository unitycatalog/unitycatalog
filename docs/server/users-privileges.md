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

`READ METADATA` permits full metadata reads and viewing all direct grants. It can be granted on a
metastore, catalog, schema, table (including views), function, volume, registered model, external
location, or storage credential. Metastore, catalog, and schema grants cover descendants. Metastore
and catalog grants need no usage grants. A schema grant needs `USE CATALOG`. A table, function,
volume, or model grant needs both `USE CATALOG` and `USE SCHEMA`. It does not permit data access,
inference, temporary credentials, writes, or permission changes.

## Viewing Grants

`permission get` lists the privileges granted directly on an object. Add `--principal` to list only
one user's grants:

```sh
bin/uc --auth_token $(cat etc/conf/token.txt) permission get --securable_type catalog --name unity --principal bobbie@rocinante
```

`--principal` must match the user's email exactly as the result shows it. An email with no grants
on the object, or one that is not a user, returns an empty list.

These users see every user's grants on an object:

- The metastore admin, and users with `READ METADATA` on the metastore. `MANAGE` cannot be granted
  on the metastore.
- Owners of the object, or of the schema or catalog that contains it.
- For a catalog, external location, or storage credential: users with `MANAGE` or `READ METADATA`
  on it.
- For a schema: users with `MANAGE` or `READ METADATA` on its catalog, or on the schema together
  with `USE CATALOG`.
- For a table, function, volume, or registered model: users with `MANAGE` or `READ METADATA` on its
  catalog, on its schema together with `USE CATALOG`, or on the object together with `USE CATALOG`
  and `USE SCHEMA`.

Other users see only their own grants, so for them any other email returns an empty list.
