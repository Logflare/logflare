defmodule Logflare.Users do
  alias Logflare.User
  alias Logflare.Repo
  @moduledoc false

  def get_by(keyword) do
    User
    |> Repo.get_by(keyword)
    |> default_preloads()
  end

  def get_by_id(id), do: get_by(id: id)

  def default_preloads(user) do
    user
    |> Repo.preload(:sources)
  end
end
