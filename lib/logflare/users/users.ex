defmodule Logflare.Users do
  alias Logflare.{User, Source}
  import Ecto.Query
  alias Logflare.Repo
  @moduledoc false

  def get_by(keyword) do
    User
    |> Repo.get_by(keyword)
    |> Repo.preload(:sources)
  end

  def user_owns_token?(token_param) when is_binary(token_param) do

  end

  def list_source_ids(user) when is_integer(user) do
    get_by(id: user.id)
    |> list_source_ids()
  end

  def list_source_ids(user) do
    user
    |> Map.get(:sources)
    |> Enum.map(& &1.token)
  end

  def get_sources(%User{id: user_id}) do
    from(s in Source,
      where: s.user_id == ^user_id,
      order_by: [desc: s.favorite],
      # TODO: maybe order by latest events?
      order_by: s.name
    )
    |> Repo.all()
    |> Repo.preload(:user)
  end
end
