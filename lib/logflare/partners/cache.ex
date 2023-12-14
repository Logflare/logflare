defmodule Logflare.Partners.Cache do
  @moduledoc """
  Cache for Partners
  """

  alias Logflare.Partners

  def child_spec(_) do
    stats = Application.get_env(:logflare, :cache_stats, false)
    %{id: __MODULE__, start: {Cachex, :start_link, [__MODULE__, [stats: stats, limit: 100_000]]}}
  end

  def get_partner(id), do: apply_repo_fun(__ENV__.function, [id])
  def get_user_by_token(partner, token), do: apply_repo_fun(__ENV__.function, [partner, token])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Partners, arg1, arg2)
  end
end
