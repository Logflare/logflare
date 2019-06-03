defmodule Logflare.Users do
  alias Logflare.{Source}
  import Ecto.Query
  alias Logflare.Repo
  @moduledoc false

  def get_by(keyword) do
    User
    |> Repo.get_by(keyword)
    |> default_preloads()
  end

  def default_preloads(user) do
    user
    |> Repo.preload(:sources)
  end
end
