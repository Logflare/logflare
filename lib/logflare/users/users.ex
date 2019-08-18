defmodule Logflare.Users do
  alias Logflare.{User, Repo, Sources, Users}
  alias Logflare.Repo
  alias Logflare.Sources
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

  def get_by_source(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.Cache.get_by_id(source_id)
    Users.Cache.get_by_id(user_id)
  end
end
