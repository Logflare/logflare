defmodule Logflare.Users.Cache do
  alias Logflare.Users
  @cache __MODULE__

  def get_by_id(id) do
    case Cachex.fetch(@cache, id, &Users.get_user_by_id/1) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end

  def list_sources(id) do
    id
    |> get_by_id()
    |> Map.get(:sources)
  end

  def get_api_quotas(user_id, source_id) do
    user = get_by_id(user_id)

    source = Enum.find(user.sources, &(String.to_atom(&1.token) == source_id))

    cond do
      is_nil(user) ->
        {:error, :user_is_nil}

      is_nil(source) ->
        {:error, :source_is_nil}

      true ->
        {:ok, %{
          user: user.api_quota,
          source: source.api_quota
        }}
    end
  end
end
