defmodule Logflare.Auth.Cache do
  @moduledoc """
  Cache for Authorization context. The keys for this cache expire in the defined
  Cachex `expiration`.
  """

  alias Logflare.Auth
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.User
  alias Logflare.Utils

  require Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min(5, 2)
           ]
         ]}
    }
  end

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
