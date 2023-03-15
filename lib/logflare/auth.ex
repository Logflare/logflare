defmodule Logflare.Auth do
  @moduledoc "Authorization context"
  import Ecto.Query

  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.OauthAccessTokens.PartnerOauthAccessToken
  alias Logflare.Partners.Partner
  alias Logflare.Repo
  alias Logflare.Teams.Team
  alias Logflare.User
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

  @doc "List Oauth access tokens by user"
  @spec list_valid_access_tokens(User.t()) :: [OauthAccessToken.t()]
  def list_valid_access_tokens(%User{id: user_id}) do
    Repo.all(
      from(t in OauthAccessToken, where: t.resource_owner_id == ^user_id and is_nil(t.revoked_at))
    )
  end

  @doc "List Oauth access tokens by partner"
  @spec list_valid_partner_access_tokens(Partner.t()) :: [OauthAccessToken.t()]
  def list_valid_partner_access_tokens(%Partner{id: user_id}) do
    Repo.all(
      from(t in PartnerOauthAccessToken,
        where: t.resource_owner_id == ^user_id and is_nil(t.revoked_at)
      )
    )
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
    with {:ok, token} = ExOauth2Provider.AccessTokens.create_token(user, %{}, env_oauth_config()) do
      token
      |> Ecto.Changeset.cast(attrs, [:scopes, :description])
      |> Repo.update()
    end
  end

  def create_access_token(%Partner{} = partner, attrs) do
    with {:ok, token} =
           ExOauth2Provider.AccessTokens.create_token(partner, %{}, env_partner_oauth_config()) do
      token
      |> Ecto.Changeset.cast(attrs, [:scopes, :description])
      |> Repo.update()
    end
  end

  @doc """
  Verifies a  `%OauthAccessToken{}` or string access token.
  If valid, returns the token's associated user.

  Optionally validates if the access token needs to have any required scope.
  """
  @spec verify_access_token(OauthAccessToken.t() | String.t(), String.t() | [String.t()]) ::
          {:ok, User.t()} | {:error, term()}
  def verify_access_token(token, scopes \\ [])

  def verify_access_token(token, scope) when is_binary(scope) do
    verify_access_token(token, [scope], env_oauth_config())
  end

  def verify_access_token(%OauthAccessToken{token: str_token}, scopes) do
    verify_access_token(str_token, scopes, env_oauth_config())
  end

  def verify_access_token("" <> str_token, required_scopes)
      when is_list(required_scopes) do
    verify_access_token(str_token, required_scopes, env_oauth_config())
  end

  @doc """
  Verifies a  `%PartnerOauthAccessToken{}` or string access token.
  If valid, returns the token's associated partner.

  Optionally validates if the access token needs to have any required scope.
  """
  @spec verify_partner_access_token(
          PartnerOauthAccessToken.t() | String.t(),
          String.t() | [String.t()]
        ) ::
          {:ok, Partner.t()} | {:error, term()}
  def verify_partner_access_token(token, scopes \\ [])

  def verify_partner_access_token(token, scope) when is_binary(scope) do
    verify_access_token(token, [scope], env_partner_oauth_config())
  end

  def verify_partner_access_token(%PartnerOauthAccessToken{token: str_token}, scopes) do
    verify_access_token(str_token, scopes, env_partner_oauth_config())
  end

  def verify_partner_access_token("" <> str_token, required_scopes)
      when is_list(required_scopes) do
    verify_access_token(str_token, required_scopes, env_partner_oauth_config())
  end

  defp verify_access_token(str_token, required_scopes, config) do
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

  defp get_resource_owner_by_id(User, id), do: Logflare.Users.get(id)
  defp get_resource_owner_by_id(Partner, id), do: Logflare.Partners.get(id)

  defp check_scopes(token_scopes, required) do
    cond do
      "private" in token_scopes -> :ok
      required == [] -> :ok
      Enum.any?(token_scopes, fn scope -> scope in required end) -> :ok
      true -> {:error, :unauthorized}
    end
  end

  @doc "Revokes a given access token."
  @spec revoke_access_token(OauthAccessToken.t() | binary()) :: :ok | {:error, term()}
  def revoke_access_token(token) when is_binary(token) do
    ExOauth2Provider.AccessTokens.get_by_token(token)
    |> revoke_access_token()
  end

  def revoke_access_token(%OauthAccessToken{} = token) do
    with {:ok, _token} = ExOauth2Provider.AccessTokens.revoke(token, env_oauth_config()) do
      :ok
    end
  end
end
