defmodule Logflare.Backends.Cache do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Utils

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)

    %{
      id: __MODULE__,
      start:
        {Cachex, :start_link,
         [__MODULE__, [stats: stats, expiration: Utils.cache_expiration_min(), limit: 100_000]]}
    }
  end

  def list_backends(source), do: apply_repo_fun(:list_backends, [source])
  def get_backend_by(kv), do: apply_repo_fun(:get_backend_by, [kv])
  def get_backend(arg), do: apply_repo_fun(:get_backend, [arg])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Backends, arg1, arg2)
  end
end
