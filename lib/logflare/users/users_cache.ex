defmodule Logflare.Users.Cache do
  alias Logflare.Users
  @cache __MODULE__

  def get_by_id(id) do
    case Cachex.fetch(@cache, id, fn id ->
           {:commit, Users.get_user_by_id(id)}
         end) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end

  def list_source_ids(id) do
    id
    |> get_by_id()
    |> Map.get(:sources)
    |> Enum.map(& &1.token)
  end

  @spec get_api_quotas(integer, atom) :: {:ok, %{user: integer, source: integer}} | {:error, term}
  def get_api_quotas(user_id, source_id) when is_atom(source_id) do
    user = get_by_id(user_id)

    source = Enum.find(user.sources, &(&1.token == source_id))

    cond do
      is_nil(user) ->
        {:error, :user_is_nil}

      is_nil(source) ->
        {:error, :source_is_nil}

      true ->
        {:ok,
         %{
           user: user.api_quota,
           source: source.api_quota
         }}
    end
  end
end
