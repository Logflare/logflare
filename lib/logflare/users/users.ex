defmodule Logflare.Users do
  alias Logflare.{User, Repo, Sources, Users}
  alias Logflare.Repo
  alias Logflare.Sources
  @moduledoc false

  def get(user_id) do
    User
    |> Repo.get(user_id)
  end

  def get_by(keyword) do
    User
    |> Repo.get_by(keyword)
  end

  def get_by_and_preload(keyword) do
    User
    |> Repo.get_by(keyword)
    |> preload_defaults()
  end

  def preload_defaults(user) do
    user
    |> Repo.preload(:sources)
  end

  def get_by_source(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: user_id} = Sources.get_by(token: source_id)
    Users.get_by_and_preload(id: user_id)
  end
end
