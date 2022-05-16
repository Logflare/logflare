defmodule Logflare.Auth do
  @max_age_default 86_400

  alias Phoenix.Token
  alias Logflare.User
  alias Logflare.Teams.Team
  alias Logflare.OauthAccessTokens.OauthAccessToken

  @salt Application.get_env(:logflare, LogflareWeb.Endpoint)[:secret_key_base]
  @oauth_config Application.get_env(:logflare, ExOauth2Provider)

  def gen_email_token(email) when is_binary(email) do
    Token.sign(LogflareWeb.Endpoint, @salt, email)
  end

  def gen_email_token(id) when is_integer(id) do
    Token.sign(LogflareWeb.Endpoint, @salt, id)
  end

  def gen_email_token(map) when is_map(map) do
    Token.sign(LogflareWeb.Endpoint, @salt, map)
  end

  @spec verify_email_token(nil | binary, any) ::
          {:error, :expired | :invalid | :missing} | {:ok, any}
  def verify_email_token(token, max_age \\ @max_age_default) do
    Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: max_age)
  end

  def gen_gravatar_link(email) do
    hash = :crypto.hash(:md5, String.trim(email)) |> Base.encode16(case: :lower)
    "https://www.gravatar.com/avatar/" <> hash
  end

  @doc """
  Creates an Oauth access token with no expiry, linked to the given user or team's user.
  """
  @spec create_access_token(%Team{} | %User{}) :: {:ok, %OauthAccessToken{}} | {:error, term()}
  def create_access_token(%Team{user_id: user_id}) do
    user_id
    |> Logflare.Users.get()
    |> create_access_token()
  end

  def create_access_token(%User{} = user) do
    ExOauth2Provider.AccessTokens.create_token(user, %{}, @oauth_config)
  end

  @doc """
  Verifies a  `%OauthAccessToken{}` or string access token. If valid, returns the token's associated user.
  """
  @spec verify_access_token(%OauthAccessToken{} | binary()) :: {:ok, %User{}} | {:error, term()}
  def verify_access_token(%OauthAccessToken{token: str_token}) do
    verify_access_token(str_token)
  end

  def verify_access_token(str_token) when is_binary(str_token) do
    case ExOauth2Provider.authenticate_token(str_token, @oauth_config) do
      {:ok, %{resource_owner_id: user_id}} ->
        user = Logflare.Users.get(user_id)
        {:ok, user}

      {:error, _reason} = err ->
        err
    end
  end

  @doc """
  Revokes a given access token.
  """
  @spec revoke_access_token(%OauthAccessToken{} | binary()) :: :ok | {:error, term()}
  def revoke_access_token(token) when is_binary(token) do
    ExOauth2Provider.AccessTokens.get_by_token(token)
    |> revoke_access_token()
  end
  def revoke_access_token(%OauthAccessToken{} = token) do
    with {:ok, _token} = ExOauth2Provider.AccessTokens.revoke(token, @oauth_config) do
      :ok
    end
  end
end
