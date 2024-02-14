defmodule Logflare.Auth do
  @moduledoc "Authorization context"
  import Ecto.Query

  alias Ecto.Changeset
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.OauthAccessTokens.PartnerOauthAccessToken
  alias Logflare.Partners
  alias Logflare.Partners.Partner
  alias Logflare.Repo
  alias Logflare.Teams.Team
  alias Logflare.User
  alias Logflare.Users
  alias Phoenix.Token

  @max_age_default 86_400
  defp env_salt, do: Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
  defp env_oauth_config, do: Application.get_env(:logflare, ExOauth2Provider)
  defp env_partner_oauth_config, do: Application.get_env(:logflare, ExOauth2ProviderPartner)

  @doc "Generates an email token, accepting either an email or id"
  @spec gen_email_token(String.t() | number) :: String.t()
  def gen_email_token(email_or_id) when is_binary(email_or_id) or is_integer(email_or_id) do
    Token.sign(LogflareWeb.Endpoint, env_salt(), email_or_id)
  end

  @doc "Verifies the email token"
  @spec verify_email_token(nil | binary, any) ::
          {:error, :expired | :invalid | :missing} | {:ok, any}
  def verify_email_token(token, max_age \\ @max_age_default) do
    Token.verify(LogflareWeb.Endpoint, env_salt(), token, max_age: max_age)
  end

  @doc "Generates a gravatar link based on the user's email"
  @spec gen_gravatar_link(String.t()) :: String.t()
  def gen_gravatar_link(email) do
    hash = :crypto.hash(:md5, String.trim(email)) |> Base.encode16(case: :lower)
    "https://www.gravatar.com/avatar/" <> hash
  end

  @doc "List Oauth access tokens by user or partner"
  @spec list_valid_access_tokens(User.t() | Partner.t()) :: [OauthAccessToken.t()]
  def list_valid_access_tokens(%User{id: user_id}) do
    Repo.all(
      from(t in OauthAccessToken,
        where: t.resource_owner_id == ^user_id and is_nil(t.revoked_at),
        where: not ilike(t.scopes, "%partner%")
      )
    )
  end

  def list_valid_access_tokens(%Partner{id: partner_id}) do
    Repo.all(
      from(t in PartnerOauthAccessToken,
        where: t.resource_owner_id == ^partner_id and is_nil(t.revoked_at),
        where: ilike(t.scopes, "%partner%")
      )
    )
  end

  @doc """
  Retrieves access token struct by token value.
  Requires resource owner to be provided.

  """
  def get_access_token(%User{}, token) when is_binary(token) do
    ExOauth2Provider.AccessTokens.get_by_token(token, env_oauth_config())
  end

  def get_access_token(%Partner{}, token) when is_binary(token) do
    ExOauth2Provider.AccessTokens.get_by_token(token, env_partner_oauth_config())
  end

  @doc "Creates an Oauth access token with no expiry, linked to the given user or team's user."
  @typep create_attrs :: %{description: String.t()} | map()
  @spec create_access_token(Team.t() | User.t() | Partner.t(), create_attrs()) ::
          {:ok, OauthAccessToken.t()} | {:error, term()}
  def create_access_token(user_or_team_or_partner, attrs \\ %{})

  def create_access_token(%Team{user_id: user_id}, attrs) do
    user_id
    |> Logflare.Users.get()
    |> create_access_token(attrs)
  end

  def create_access_token(%User{} = user, attrs) do
    with {:ok, token} <- ExOauth2Provider.AccessTokens.create_token(user, %{}, env_oauth_config()) do
      token
      |> Changeset.cast(attrs, [:scopes, :description])
      |> Repo.update()
    end
  end

  def create_access_token(%Partner{} = partner, attrs) do
    with {:ok, token} <-
           ExOauth2Provider.AccessTokens.create_token(partner, %{}, env_partner_oauth_config()) do
      token
      |> Changeset.cast(attrs, [:scopes, :description])
      |> Changeset.update_change(:scopes, fn scopes -> scopes <> " partner" end)
      |> Repo.update()
    end
  end

  @doc """
  Verifies a `%OauthAccessToken{}` or string access token.
  If valid, returns the token's associated user.

  Optionally validates if the access token needs to have any required scope.
  """
  @spec verify_access_token(OauthAccessToken.t() | String.t(), String.t() | [String.t()]) ::
          {:ok, User.t()} | {:error, term()}
  def verify_access_token(token, scopes \\ [])

  def verify_access_token(token, scope) when is_binary(scope) do
    verify_access_token_and_fetch_owner(token, [scope])
  end

  def verify_access_token(%OauthAccessToken{token: str_token}, scopes) do
    verify_access_token_and_fetch_owner(str_token, scopes)
  end

  def verify_access_token(%PartnerOauthAccessToken{token: str_token}, scopes) do
    case "partner" in scopes do
      true -> verify_access_token_and_fetch_owner(str_token, scopes)
      false -> {:error, :unauthorized}
    end
  end

  def verify_access_token("" <> str_token, required_scopes) when is_list(required_scopes) do
    verify_access_token_and_fetch_owner(str_token, required_scopes)
  end

  defp verify_access_token_and_fetch_owner(str_token, required_scopes) do
    config =
      case "partner" in required_scopes do
        true -> env_partner_oauth_config()
        false -> env_oauth_config()
      end

    resource_owner = Keyword.get(config, :resource_owner)

    with {:ok, access_token} <- ExOauth2Provider.authenticate_token(str_token, config),
         token_scopes <- String.split(access_token.scopes || ""),
         :ok <- check_scopes(token_scopes, required_scopes),
         owner <- get_resource_owner_by_id(resource_owner, access_token.resource_owner_id) do
      {:ok, owner}
    else
      {:scope, false} -> {:error, :unauthorized}
      err -> err
    end
  end

  defp get_resource_owner_by_id(User, id), do: Users.Cache.get(id)
  defp get_resource_owner_by_id(Partner, id), do: Partners.Cache.get_partner(id)

  defp check_scopes(token_scopes, required) do
    cond do
      "private" in token_scopes -> :ok
      Enum.empty?(required) -> :ok
      Enum.any?(token_scopes, fn scope -> scope in required end) -> :ok
      true -> {:error, :unauthorized}
    end
  end

  @doc "Revokes a given access token."
  @spec revoke_access_token(User.t() | Partner.t(), OauthAccessToken.t() | binary()) ::
          :ok | {:error, term()}
  def revoke_access_token(user, token) when is_binary(token) do
    get_access_token(user, token)
    |> revoke_access_token()
  end

  def revoke_access_token(partner, token) when is_binary(token) do
    get_access_token(partner, token)
    |> revoke_access_token()
  end

  def revoke_access_token(%OauthAccessToken{} = token) do
    with {:ok, _token} <- ExOauth2Provider.AccessTokens.revoke(token, env_oauth_config()) do
      :ok
    end
  end
end
