defmodule Logflare.Users.Cache do
  alias Logflare.{Users, User}
  import Cachex.Spec
  @ttl 500

  @cache __MODULE__

  def child_spec(_) do
    cachex_opts = [
      expiration: expiration(default: @ttl)
    ]

    %{
      id: :cachex_users_cache,
      start: {Cachex, :start_link, [Users.Cache, cachex_opts]}
    }
  end

  def get_by(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def get_by_id(id), do: get_by(id: id)
  def get_api_quotas(keyword), do: apply_repo_fun(__ENV__.function, [keyword])

  def delete_cache_key_by_id(id) do
    {:ok, _} = Cachex.del(@cache, {{:get_by, 1}, [[id: id]]})
  end

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_repo_fun(Users, arg1, arg2)
  end
end
