# Provision Users and Groups from Your Identity Provider

This guide connects an identity provider (IdP) to Unity Catalog so that the IdP creates and updates users and groups
over SCIM, and the people it provisions sign in with the IdP. The steps use Okta. The Unity Catalog side is the same
for any IdP that can send SCIM requests with a fixed bearer token.

Read [Auth](auth.md) first for how Unity Catalog authenticates and authorizes requests.

## 1. Configure Unity Catalog

Turn on authorization and set the authentication type to `external`. In external mode people send IdP access tokens
directly, and the server validates them against the IdP's signing keys.

```properties
server.authorization=enable
server.authentication.type=external

server.authentication.idp.okta.issuer=https://acme.okta.com/oauth2/aus1example
server.authentication.idp.okta.audience=unity-catalog
server.authentication.idp.okta.subject-claim=OKTA
```

Each IdP has its own block, named `server.authentication.idp.<name>.*`, set in `server.properties`:

| Property | Meaning |
| --- | --- |
| `issuer` | The exact issuer (`iss`) of the IdP's access tokens. For Okta, use the issuer of a custom authorization server. |
| `audience` | The `aud` value the IdP puts in access tokens for Unity Catalog. |
| `subject-claim` | The token claim that identifies a person after the first sign-in. `SUB` (the default) uses the standard `sub` claim. `OKTA` uses the user id (`uid`) claim for people and the client id (`cid`) claim for machines, because an Okta admin can map the Okta `sub` claim to any value. |

In external mode only these per-IdP blocks decide which issuers and audiences are accepted. `server.allowed-issuers` and
`server.audiences` apply only to internal mode (the default). The server logs a warning at startup if you set them in
external mode. See [External authentication mode](auth.md#external-authentication-mode) for the full rules.

The `<name>` you choose here (`okta` in the example) is also the name you pass to `--idp` when you mint the SCIM token
in the next step. It can't be `admin`.

Okta access tokens do not carry the user's email by default. Add an `email` claim to the access tokens of your custom
authorization server (Security > API > Authorization Servers > your server > Claims, include in the access token,
value `user.email`). Unity Catalog needs it once, on a person's first sign-in, to find their user row (see
[First sign-in](#first-sign-in)).

Unity Catalog identifies a user by email and refuses a second user with the same email (see [Name clashes and
ownership](#name-clashes-and-ownership)). Okta does not always refuse two people with the same email, so keep emails
unique in Okta.

Restart the server after changing these settings.

## 2. Create the IdP's SCIM principal

The IdP calls SCIM as its own Unity Catalog user, the SCIM principal. Create the user with the admin token, then mint a
SCIM token for it.

```sh
bin/uc --auth_token "$(bin/uc-admin-token)" user create --name "Okta SCIM" --email okta-scim
```

```sh
bin/uc-admin-token --scim-principal okta-scim --idp okta --lifetime P90D
```

- `--scim-principal` is the email of the user you just created.
- `--idp` is the IdP name from your `server.authentication.idp.<name>` block.
- `--lifetime` is an ISO-8601 duration (here 90 days). It is required, and the token stops working when it elapses.
  To rotate, mint a new token, paste it into the IdP, and let the old one expire.

`bin/uc-admin-token` opens the server's database itself. On the default H2 database only one process can open the
file, so stop the server before you run it, then start it again. H2 is meant for development only. See
[Give an identity provider a SCIM token](auth.md#give-an-identity-provider-a-scim-token) for the other conditions.

<!-- TODO(PR 3): a delete must remove the user row, so that a user created again with the same email gets a new id
and the old SCIM tokens stay invalid. Today a delete only disables the user. -->
To pause the IdP, disable its principal. To revoke its tokens for good, delete it. This works once a delete removes the
user: a user created again with the same email then gets a new id. Today a delete only disables the user, so creating it
again gets 409. See [Give an identity provider a SCIM token](auth.md#give-an-identity-provider-a-scim-token) for what
the token can reach.

Treat the token like a password. Anyone who holds it can create and change the users and groups that the IdP owns.

## 3. Add Unity Catalog as an app in Okta

These steps create the app yourself with Okta's App Integration Wizard. See Okta's [Add SCIM provisioning to app
integrations](https://help.okta.com/en-us/content/topics/apps/apps_app_integration_wizard_scim.htm) for details.

1. In the Admin Console, go to **Applications**, click **Create App Integration**, and create a SAML or SWA app
   integration.
2. On its **General** tab, edit **App Settings**, set **Provisioning** to **SCIM**, and save.
3. On its **Provisioning** tab, edit the **Integration** settings:

| Okta setting | Value |
| --- | --- |
| SCIM connector base URL | `https://<your-uc-host>/api/1.0/unity-control/scim2` |
| Unique identifier field for users | `userName` |
| Supported provisioning actions | **Push New Users**, **Push Profile Updates**, **Push Groups** |
| Authentication Mode | HTTP Header |
| Authorization | The token printed by `bin/uc-admin-token` |

Unity Catalog accepts the token only in an `Authorization: Bearer <token>` header. Paste only the token into the
**Authorization** field, without the word `Bearer`: Okta shows that prefix next to the field (see [this Okta support
article](https://support.okta.com/help/s/article/error-authenticating-unauthorized?language=en_US)).

Save the integration settings. Then, under **Provisioning > To App**, turn on:

- **Create Users**
- **Update User Attributes**
- **Deactivate Users**

Unity Catalog uses the person's email as the SCIM `userName`, so the username Okta sends must be the email. On the
app's **Sign On** tab, set **Application username format** to a format that gives the person's email (see [Okta's
article on this setting](https://support.okta.com/help/s/article/Update-application-username-using-an-expression-language?language=en_US)).

<!-- TODO(PR 2): the user requests Okta sends must work: the userName filter with startIndex and count, PUT with the
full user (also to set active), and PATCH with {"op":"replace","value":{"active":false}}. -->
Okta never sends `DELETE` for a user. When you unassign a user, Okta deactivates the row. When you assign the user
again, Okta reactivates the same row, and the person's next sign-in links it again (see [First
sign-in](#first-sign-in)). Unity Catalog does not store the `password` that Okta sends in `POST /Users`.

## 4. Assign users, then push groups

Assigning is not pushing. Do the two steps in this order.

1. **Assign users.** On the app's **Assignments** tab, assign people, or use **Assign to Groups** to assign everyone in
   an Okta group. Okta creates a Unity Catalog user for each assigned person. Assigning a group only provisions its
   members; it does not create the group in Unity Catalog.
2. **Push groups.** On the app's **Push Groups** tab, push groups by name or by rule. Okta creates each group with
   `POST /Groups` and then sends its members. A member must be a user that Okta provisioned, so push groups after the
   users are assigned. A request that names any other member fails with `400` and changes nothing.

The groups Okta pushes are owned by `okta`. Rename or delete them in Okta: the admin can't rename, replace or delete
them in Unity Catalog. The admin can still add and remove members, in a separate member list (see [Name clashes and
ownership](#name-clashes-and-ownership)).

<!-- TODO(PR 7): grants to a group name must apply to the group's members. -->
Grant privileges to the pushed groups and to users in the usual way. See [Users and Privileges](users-privileges.md).

## Name clashes and ownership

Every user and group has an owner: `admin` for rows created with the admin token or the CLI, or the IdP name (for
example `okta`) for rows the IdP creates or that a person's first sign-in creates. SCIM responses show it in the
`urn:unitycatalog:scim:schemas:extension:2.0:Owner` extension. See [Users and groups an identity provider
owns](auth.md#users-and-groups-an-identity-provider-owns).

- **Names are unique.** User emails and group names are unique across all owners. If Okta creates a user or group whose
  name already exists, Unity Catalog answers `409 Conflict` and Okta reports a provisioning error. Rename or delete
  one side, then retry.
- **Each side edits only its own rows.** The IdP can see and change only the rows it owns. The admin sees every row,
  but can't update, deactivate or delete a user that an IdP owns (`403`). A row created by the admin is invisible to
  the IdP, so a user the admin created by hand is not adopted by Okta; Okta tries to create it and gets a `409`.
<!-- TODO(PR 3): a delete must remove the user row; today it only disables the user, so Okta's create gets 409. -->
- **Existing users.** Users that existed before you upgraded belong to `admin`. To move such a user under Okta, delete
  the user in Unity Catalog, let Okta create it again, and grant its privileges again. This works once a delete removes
  the user; today a delete only disables it, so Okta's create gets 409.

A group has two member lists: the members the IdP pushed, and the members an admin added. A user is a member if it is
in either. When Okta pushes a group, it changes only its own list, so a member that an admin added stays after the
next push.

<!-- TODO(PR 7): a grant must resolve its principal as a user's email first, then a group's name. -->
When you grant a privilege to a name, Unity Catalog looks for a user with that email first and a group with that name
second. A user whose email equals a group's name therefore shadows the group for grants. If this happens, rename the
group (in Okta, for a pushed group) to manage its grants.

## First sign-in

People sign in with the IdP, not with Unity Catalog. Unity Catalog only sees the IdP's access tokens.

On a person's first call with an IdP access token, the server finds their user row by the token's `email` claim and
links the row to the IdP identity in the token (the issuer plus the claim named by `subject-claim`). From then on it
recognizes the person by that identity and not by email. An IdP-owned row links only through its own IdP. The server
never links a disabled row. When the IdP reactivates a user, the server clears the link, and the next sign-in links
the row again by email, with a token issued at or after the reactivation. See [External authentication
mode](auth.md#external-authentication-mode) for the full rules.

A person who signs in through a configured IdP but has no user row gets one, owned by that IdP. The new user has no
access until you grant them privileges directly or through a group.

<!-- TODO(PR 12): in external mode, bin/uc auth login must print the IdP's access token when the token exchange
answers 501. -->
<!-- TODO(PR 13): ui-new must show "Sign in with Okta" when OKTA_AUTH_ENABLED, OKTA_ISSUER and OKTA_CLIENT_ID are set,
and send the Okta access token when the token exchange is disabled. -->
- **CLI.** Run `bin/uc auth login`. In external mode it prints the IdP's access token, because the exchange is disabled.
  Pass that token to later commands with `bin/uc --auth_token <token>`.
- **UI.** The new UI shows a **Sign in with Okta** button when its bridge has `OKTA_AUTH_ENABLED=true`, `OKTA_ISSUER`
  (the same authorization server as `issuer` above) and `OKTA_CLIENT_ID` (an Okta single-page app that uses PKCE).
- **Existing token.** You can also pass an IdP access token yourself with `bin/uc --auth_token <token>`, or paste it
  into the UI.

In external mode `POST /auth/tokens` is not implemented and returns `501`: token exchange is disabled. The server's own
tokens work only for the bootstrap admin token and the SCIM principal's token.

## Deactivating a person

<!-- TODO(PR 2): PUT /Users/{id} with the full user and active false, and PATCH /Users/{id} with
{"op":"replace","value":{"active":false}}, must disable the user. -->
Deactivate the person in Okta, or unassign them from the app. Okta sends `active: false` in `PUT /Users/{id}` (`PATCH`
for an Okta Integration Network app), and the server disables the user. From the next request it refuses every token
for that user, including IdP access tokens that have not expired yet.
