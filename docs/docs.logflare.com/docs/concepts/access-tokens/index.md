---
sidebar_position: 4
---

# Access Tokens

Logflare uses access tokens for user API authentication. These access tokens must be provided with each API requests, with the exception of resources that have authentication disabled by the user.

[Access Tokens Dashboard](https://logflare.app/account/access-tokens)

## Scopes

Access tokens can be scoped to specific functionality.

1. `ingest` - allows ingestion of events into sources.
2. `query` - allows querying of Logflare Endpoints.
3. `private` - allows management of team resources 
4. `private:admin` allows management of team resources and team users.

`ingest` and `query` scopes can be scoped to specific sources and endpoints. It is recommended to use the least privileged token for the given task, especially for public ingestion.

## Managing Access Tokens

Access tokens can be created under the [Manage Access Tokens page](https://logflare.app/account/access-tokens).

First, click on **Create access token** button.

![Click on the Create access token button](./create-access-token-button.png)

Then, enter a description for the token for reference. Click on **Create** once you are done.

![Enter a description](./create-description.png)

You will be shown the access token only **once**. Do copy the token to a safe location.
 
![Example token](./create-success.png)

To revoke access tokens, click on the **Revoke** button. This would immediately reject all incoming API requests.

![Revoke token](./revoke.png)

## Authentication

There are 3 supported methods to attach an access token with an API request:

1. Using the `Authorization` header, with the format `Authorization: Bearer your-access-token-here`
2. Using the `X-API-KEY` header, with the format `X-API-KEY: your-access-token-here`
3. Using the `api_key` query parameter, with the format `?api_key=your-access-token-here`

## Client-side is Public

Access tokens can be exposed for client-side usage. Consider restricting tokens to ingest into specific sources or to considered public for client-side usage. Public-only tokens will not be able to access the Logflare Management API beyond the ingest/query APIs.

Private access tokens should not be exposed to the client side, as private access tokens have complete access to all management APIs used for account control.

## Rotation

When rotating access tokens for client-side access tokens, we recommend creating another client-side token and performing a gradual shift to the new token before revoking the old token.

Revoking the old token while clients are still on it would result in API request errors on the client.
