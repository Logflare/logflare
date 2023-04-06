---
sidebar_position: 4
---

# Access Tokens

Logflare uses access tokens for user API authentication. These access tokens must be provided with each API requests, with the exception of resources that have authentication disabled by the user

## Managing Access Tokens

Access tokens can be created under the [Manage Access Tokens page](https://logflare.app/account/access-tokens).

First, click on **Create access token** button.

![Click on the Create access token button](./create-access-token-button.png)

Then, enter a description for the token for reference. Click on **Create** once you are done.

![Enter a description](./create-description.png)

You will be shown the access token ony **once**. Do copy the token to a safe location.

![Example token](./create-success.png)

To revoke access tokens, clikc on the **Revoke** button. This would immediately reject all incoming API requests.

![Revoke token](./revoke.png)

## Authentication

There are 3 supported methods to attach an accees token with an API request:

1. Using the `Authorization` header, with the format `Authorization: Bearer your-access-token-here`
2. Using the `X-API-KEY` header, with the format `X-API-KEY: your-access-token-here`
3. Using the `api_key` query parameter, wuth the format `?api_key=your-access-token-here`

## Client-side is Public

Access tokens are considered public for client-side usage. Public-only tokens will not be able to access the Logflare Management API, which provides capabilities to manage user resources on Logflare.

Please use a separate private access token for the Management API.

## Rotation

When rotating access tokens for client-side access tokens, we recommend creating another client-side token and performing a gradual shift to the new token before revoking the old token.

Revoking the old token while clients are still on it would result in API request errors on the client.
