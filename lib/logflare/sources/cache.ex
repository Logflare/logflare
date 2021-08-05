defmodule Logflare.Sources.Cache do
  @moduledoc false

  alias Logflare.Sources

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: true, limit: 100_000]]}}
  end

  def get_by_and_preload(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def get_by_id_and_preload(arg) when is_integer(arg), do: get_by_and_preload(id: arg)
  def get_by_id_and_preload(arg) when is_atom(arg), do: get_by_and_preload(token: arg)
  def get_by_name_and_preload(arg) when is_binary(arg), do: get_by_and_preload(name: arg)
  def get_by_pk_and_preload(arg), do: get_by_and_preload(id: arg)
  def get_by_public_token_and_preload(arg), do: get_by_and_preload(public_token: arg)

  def get_source_for_lv_param(source_id) when is_binary(source_id) or is_integer(source_id) do
    apply_repo_fun(__ENV__.function, [source_id])
  end

  def get_by(keyword), do: apply_repo_fun(__ENV__.function, [keyword])
  def get_by_id(arg) when is_integer(arg), do: get_by(id: arg)
  def get_by_id(arg) when is_atom(arg), do: get_by(token: arg)
  def get_by_name(arg) when is_binary(arg), do: get_by(name: arg)
  def get_by_pk(arg), do: get_by(id: arg)
  def get_by_public_token(arg), do: get_by(public_token: arg)

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Sources, arg1, arg2)
  end
end
