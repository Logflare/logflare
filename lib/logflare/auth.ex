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
  alias Logflare.Teams.TeamContext
  alias Logflare.User
  alias Logflare.Users
  alias Phoenix.Token

  @max_age_default 86_400
  @admin_scope "private:admin"

  @spec admin_scope() :: String.t()
  def admin_scope, do: @admin_scope

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

  @doc "Get valid Oauth access tokens by user or partner"
  @spec get_valid_access_token(User.t() | Partner.t(), String.t()) :: OauthAccessToken.t() | nil
  def get_valid_access_token(%User{id: user_id}, token_string) do
    Repo.one(
      from(t in OauthAccessToken,
        where: t.resource_owner_id == ^user_id and is_nil(t.revoked_at),
        where: not ilike(t.scopes, "%partner%"),
        where: t.token == ^token_string
      )
    )
  end

  def get_valid_access_token(%Partner{id: partner_id}, token_string) do
    Repo.one(
      from(t in OauthAccessToken,
        where: t.resource_owner_id == ^partner_id and is_nil(t.revoked_at),
        where: ilike(t.scopes, "%partner%"),
        where: t.token == ^token_string
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
      |> Changeset.cast(attrs, [:token, :scopes, :description])
      |> Changeset.unique_constraint(:token)
      |> Repo.update()
    end
  end

  def create_access_token(%Partner{} = partner, attrs) do
    with {:ok, token} <-
           ExOauth2Provider.AccessTokens.create_token(partner, %{}, env_partner_oauth_config()) do
      token
      |> Changeset.cast(attrs, [:token, :scopes, :description])
      |> Changeset.update_change(:scopes, fn scopes -> scopes <> " partner" end)
      |> Changeset.unique_constraint(:token)
      |> Repo.update()
    end
  end

  @doc """
  Verifies a `%OauthAccessToken{}` or string access token.
  If valid, returns the token's associated user.

  Optionally validates if the access token needs to have any required scope.
  """
  @spec verify_access_token(OauthAccessToken.t() | String.t(), String.t() | [String.t()]) ::
          {:ok, OauthAccessToken.t(), User.t()} | {:error, term()}
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
         :ok <- check_scopes(access_token, required_scopes) do
      owner = get_resource_owner_by_id(resource_owner, access_token.resource_owner_id)
      {:ok, access_token, owner}
    end
  end

  defp get_resource_owner_by_id(User, id), do: Users.Cache.get(id)
  defp get_resource_owner_by_id(Partner, id), do: Partners.Cache.get_partner(id)

  @doc """
  Checks that an access token contains any scopes that are provided in a given required scopes list.

  Private scope will always return `:ok`
  iex> check_scopes(access_token, ["private"])

  Admin scope (`private:admin`) implies private scope access:
  iex> check_scopes(%OauthAccessToken{scopes: "private:admin"}, ["private"])
  :ok

  iex> check_scopes(%OauthAccessToken{scopes: "ingest:source:1"}, ["ingest:source:1"])
  If multiple scopes are provided, each scope must be in the access token's scope string
    :ok
  only can ingest to token-specified scopes
    iex> check_scopes(%OauthAccessToken{scopes: "ingest:source:1"}, ["ingest"])
    {:error, :unauthorized}

  if scopes to check for are missing, will be unauthorized
    iex> check_scopes(%OauthAccessToken{scopes: "ingest"}, ["ingest", "source:1"])
    {:error, :unauthorized}

  """
  def check_scopes(%_{} = access_token, required_scopes) when is_list(required_scopes) do
    token_scopes = String.split(access_token.scopes || "")
    requires_admin = @admin_scope in required_scopes

    cond do
      @admin_scope in token_scopes ->
        :ok

      "private" in token_scopes and not requires_admin ->
        :ok

      Enum.empty?(required_scopes) ->
        :ok

      # legacy behaviours
      # empty scope
      Enum.empty?(token_scopes) and Enum.any?(required_scopes, &String.starts_with?(&1, "ingest")) ->
        :ok

      # deprecated scope
      "public" in token_scopes and Enum.any?(required_scopes, &String.starts_with?(&1, "ingest")) ->
        :ok

      Enum.any?(token_scopes, fn scope -> scope in required_scopes end) ->
        :ok

      true ->
        {:error, :unauthorized}
    end
  end

  @doc "Revokes a given access token."
  @spec revoke_access_token(User.t() | Partner.t(), binary()) :: :ok | {:error, term()}
  def revoke_access_token(user_or_partner, token) when is_binary(token) do
    user_or_partner
    |> get_access_token(token)
    |> revoke_access_token()
  end

  @spec revoke_access_token(OauthAccessToken.t()) :: :ok | {:error, term()}
  def revoke_access_token(%OauthAccessToken{} = token) do
    with {:ok, _token} <- ExOauth2Provider.AccessTokens.revoke(token, env_oauth_config()) do
      :ok
    end
  end

  @doc """
  Checks if a user can create access tokens with the admin scope.

  ## API Context (with OauthAccessToken)
  Token must have the `private:admin` scope.

  ## LiveView Context (with TeamContext)
  User must be signed in as team owner or team_user with role 'admin'.
  """
  @spec can_create_admin_token?(OauthAccessToken.t() | TeamContext.t()) ::
          boolean()
  def can_create_admin_token?(%TeamContext{} = team_context),
    do: TeamContext.team_admin?(team_context)

  def can_create_admin_token?(%OauthAccessToken{scopes: scopes}) when is_binary(scopes) do
    @admin_scope in String.split(scopes)
  end

  def can_create_admin_token?(_), do: false
end
