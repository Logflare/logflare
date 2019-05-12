defmodule Logflare.Users.Cache do
  alias Logflare.{Users, User}
  import Cachex.Spec
  @ttl :timer.minutes(5)

  @cache __MODULE__

  def child_spec(_) do
    cachex_opts = [
      expiration: expiration(default: @ttl)
    ]

    %{
      id: :cachex_users_cache,
      start: {Cachex, :start_link, [Users.Cache, cachex_opts]}
    }
  end

  def get_by_id(id) do
    case Cachex.fetch(@cache, id, fn id ->
           {:commit, Users.get_user_by_id(id)}
         end) do
      {:commit, value} -> value
      {:ok, value} -> value
    end
  end

  @spec source_id_owned?(User.t(), String.t()) :: User.t()
  def source_id_owned?(user, source_id) do
    source_id in Enum.map(user.sources, & &1.token)
  end

  @spec find_user_by_api_key(String.t()) :: User.t()
  def find_user_by_api_key(api_key) do
    fetch_or_commit({:api_key, [api_key]}, &Users.find_user_by_api_key/1)
  end

  def fetch_or_commit({type, args}, fun) when is_list(args) and is_atom(type) do
    case Cachex.fetch(@cache, {type, args}, fn {_type, args} ->
           {:commit, apply(fun, args)}
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

  @spec get_api_quotas(integer, atom) ::
          {:ok, %{user: integer, source: integer}} | {:error, :user_is_nil | :source_is_nil}
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
