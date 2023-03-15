defmodule Logflare.Auth do
  @moduledoc "Authorization context"
  import Ecto.Query
  alias Phoenix.Token
  alias Logflare.{Repo, User, Teams.Team, OauthAccessTokens.OauthAccessToken}
  @max_age_default 86_400
  defp env_salt, do: Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
  defp env_oauth_config, do: Application.get_env(:logflare, ExOauth2Provider)

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
      from t in OauthAccessToken, where: t.resource_owner_id == ^user_id and is_nil(t.revoked_at)
    )
  end

  @doc "Creates an Oauth access token with no expiry, linked to the given user or team's user."
  @typep create_attrs :: %{description: String.t()} | map()
  @spec create_access_token(Team.t() | User.t(), create_attrs()) ::
          {:ok, OauthAccessToken.t()} | {:error, term()}
  def create_access_token(user_or_team, attrs \\ %{})

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

  @doc """
  Verifies a  `%OauthAccessToken{}` or string access token.
  If valid, returns the token's associated user.

  Optionally validates if the access token needs to have any required scope.
  """
  @spec verify_access_token(OauthAccessToken.t() | String.t(), String.t() | [String.t()]) ::
          {:ok, User.t()} | {:error, term()}
  def verify_access_token(token, scopes \\ [])

  def verify_access_token(token, scope) when is_binary(scope) do
    verify_access_token(token, [scope])
  end

  def verify_access_token(%OauthAccessToken{token: str_token}, scopes) do
    verify_access_token(str_token, scopes)
  end

  def verify_access_token("" <> str_token, required_scopes) when is_list(required_scopes) do
    with {:ok, access_token} <-
           ExOauth2Provider.authenticate_token(str_token, env_oauth_config()),
         token_scopes <- String.split(access_token.scopes || ""),
         :ok <- check_scopes(token_scopes, required_scopes),
         user <- Logflare.Users.get(access_token.resource_owner_id) do
      {:ok, user}
    else
      {:scope, false} ->
        {:error, :unauthorized}

      err ->
        err
    end
  end

  defp check_scopes(token_scopes, required) do
    cond do
      "private" in token_scopes -> true
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
