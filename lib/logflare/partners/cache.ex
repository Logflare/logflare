defmodule Logflare.Partners.Cache do
  @moduledoc """
  Cache for Partners
  """

  use Logflare.ContextCache

  alias Logflare.Cache.CachexOps
  alias Logflare.Partners

  def child_spec(_), do: CachexOps.child_spec(__MODULE__, limit: 100_000)

  def get_partner(id), do: apply_repo_fun(__ENV__.function, [id])
  def get_user_by_uuid(partner, token), do: apply_repo_fun(__ENV__.function, [partner, token])

  defp apply_repo_fun(arg1, arg2) do
    Logflare.ContextCache.apply_fun(Partners, arg1, arg2)
  end
end
