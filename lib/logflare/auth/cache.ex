defmodule Logflare.Auth.Cache do
  @moduledoc """
  Cache for Authorization context. The keys for this cache expire in the defined
  Cachex `expiration`.
  """

  @behaviour Logflare.Cache
  @behaviour Logflare.ContextCache

  alias Logflare.Auth
  alias Logflare.Cache.CachexOps
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.User

  def child_spec(_) do
    CachexOps.child_spec(__MODULE__,
      limit: 100_000,
      ttl: to_timeout(minute: 5),
      purge_interval: to_timeout(minute: 2)
    )
  end

  @impl Logflare.Cache
  def healthy?, do: CachexOps.healthy?(__MODULE__)

  @spec verify_access_token(OauthAccessToken.t() | String.t()) ::
          {:ok, OauthAccessToken.t(), User.t()} | {:error, term()}
  def verify_access_token(access_token_or_api_key),
    do: apply_repo_fun(__ENV__.function, [access_token_or_api_key])

  @spec verify_access_token(OauthAccessToken.t() | String.t(), String.t() | [String.t()]) ::
          {:ok, OauthAccessToken.t(), User.t()} | {:error, term()}
  def verify_access_token(access_token_or_api_key, scopes),
    do: apply_repo_fun(__ENV__.function, [access_token_or_api_key, scopes])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Auth, arg1, arg2)
  end
end
