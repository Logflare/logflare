defmodule Logflare.Sources.Cache do
  @moduledoc false

  alias Logflare.Sources

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: true, limit: 100_000]]}}
  end

  # For ingest
  def get_by_and_preload_rules(kv), do: apply_repo_fun(__ENV__.function, [kv])

  def get_by_and_preload(kv), do: apply_repo_fun(__ENV__.function, [kv])
  def get_by_id_and_preload(arg) when is_integer(arg), do: get_by_and_preload(id: arg)
  def get_by_id_and_preload(arg) when is_atom(arg), do: get_by_and_preload(token: arg)

  def get_by(kv), do: apply_repo_fun(__ENV__.function, [kv])
  def get_by_id(arg) when is_integer(arg), do: get_by(id: arg)
  def get_by_id(arg) when is_atom(arg), do: get_by(token: arg)

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Sources, arg1, arg2)
  end
end
