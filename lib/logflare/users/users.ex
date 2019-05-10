defmodule Logflare.Users do
  alias Logflare.{User, Source}
  alias Logflare.Repo
  @moduledoc false

  def get_user_by_id(id) when is_integer(id) do
    User
    |> Repo.get(id)
    |> Repo.preload(:sources)
  end

  def get_api_quotas(%User{} = user, %Source{} = source) do
    source = Enum.find(user.sources, &(&1.id === source))

    %{
      user: user.api_quota,
      source: source.api_quota
    }
  end
end
