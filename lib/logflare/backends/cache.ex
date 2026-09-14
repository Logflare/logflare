defmodule Logflare.Backends.Cache do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Utils
  import Cachex.Spec

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [
           __MODULE__,
           [
             warmers: [
               warmer(required: false, module: Backends.CacheWarmer, name: Backends.CacheWarmer)
             ],
             hooks:
               [
                 if(stats, do: Utils.cache_stats()),
                 Utils.cache_limit(100_000)
               ]
               |> Enum.filter(& &1),
             expiration: Utils.cache_expiration_min()
           ]
         ]}
    }
  end

  def list_backends(arg), do: apply_repo_fun(__ENV__.function, [arg])

  @spec list_enabled_backends(keyword()) :: [Backends.Backend.t()]
  def list_enabled_backends(arg) do
    # We filter the existing cached list instead of creating a
    # `{:list_backends, [..., enabled: true]}` key. That entry would omit disabled backends, so
    # re-enabling one could not clear it by backend ID.
    arg
    |> list_backends()
    |> Enum.filter(& &1.enabled)
  end

  def get_backend(arg), do: apply_repo_fun(__ENV__.function, [arg])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
