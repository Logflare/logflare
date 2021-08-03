defmodule Logflare.Users.Cache do
  @moduledoc """
  Cache for users.
  """

  alias Logflare.Users

  def child_spec(_) do
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: true, limit: 100_000]]}}
  end

  def get_by(keyword), do: apply_repo_fun(__ENV__.function, [keyword])

  def get_by_and_preload(keyword), do: apply_repo_fun(__ENV__.function, [keyword])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Users, arg1, arg2)
  end
end
