defmodule Logflare.Users.Cache do
  alias Logflare.Users
  @cache __MODULE__

  def get_by_id(id) do
    case Cachex.fetch(@cache, id, &Users.get_user_by_id/1) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end
end
