defmodule Logflare.QueryContext do
  @moduledoc """
  Cached query dependencies with primary-backed fills and WAL invalidation.

  Transactions bypass shared snapshots. Application writes invalidate locally
  after commit; other nodes converge through WAL rather than snapshot gossip.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.QueryContext.Cache
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Users

  @spec get_user(pos_integer()) :: User.t() | nil | {:error, :database_unavailable}
  def get_user(id), do: read({:get_user, [id]}, fn -> Users.get(id) end)

  @spec get_backend(pos_integer()) :: Backend.t() | nil | {:error, :database_unavailable}
  def get_backend(id), do: read({:get_backend, [id]}, fn -> Backends.get_backend(id) end)

  @spec list_backends(pos_integer()) :: [Backend.t()] | {:error, :database_unavailable}
  def list_backends(user_id) do
    read({:list_backends, [user_id]}, fn -> Backends.list_backends(user_id: user_id) end)
  end

  @spec invalidate_user(User.t()) :: :ok
  def invalidate_user(%User{id: id}) do
    invalidate_after_commit(user_id: id)
  end

  @spec invalidate_backend(Backend.t()) :: :ok
  def invalidate_backend(%Backend{id: id, user_id: user_id}) do
    invalidate_after_commit(backend_id: id, owner_id: user_id)
  end

  defp invalidate_after_commit(fields) do
    Repo.after_commit(fn -> Cache.invalidate(fields) end)
  end

  defp read(key, loader) do
    if Repo.in_transaction?(), do: loader.(), else: Cache.fetch(key, loader)
  end
end
