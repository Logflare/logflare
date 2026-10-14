defmodule Logflare.RecordUsages do
  @moduledoc """
  Tracks recent usage separately from resource records without invalidating their caches.

  Usage is buffered locally and persisted every five minutes, with a best-effort final
  flush during graceful shutdown. Unflushed usage may still be lost on node failure or
  if the shutdown flush fails or times out. Missing usage means unknown, not never used.

  Each resource type has a unique foreign key in `record_usages` with cascading deletes.
  Adding a resource requires a nullable foreign key, a unique index, extending the
  exactly-one-resource constraint, and registering its schema and foreign key here.
  """

  import Ecto.Query

  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.RecordUsage
  alias Logflare.Repo

  @type resource_type :: OauthAccessToken

  @spec record(resource_type(), pos_integer()) :: :ok
  def record(OauthAccessToken = resource_type, id) do
    Cache.record({resource_type, id}, System.system_time(:second))
  end

  @spec persist(Cache.snapshot()) :: :ok
  def persist(entries) do
    entries
    |> Enum.group_by(fn {{resource_type, _id}, _timestamp} -> resource_type end)
    |> Enum.each(fn {resource_type, entries} -> persist_resource(resource_type, entries) end)
  end

  @spec persist_resource(resource_type(), Cache.snapshot()) :: :ok
  defp persist_resource(schema, entries) do
    foreign_key = foreign_key(schema)

    rows =
      Enum.map(entries, fn {{_type, id}, timestamp} ->
        %{
          foreign_key => id,
          :last_used_at => DateTime.from_unix!(timestamp, :second)
        }
      end)

    query =
      from(usage in values(rows, RecordUsage),
        join: resource in ^schema,
        on: resource.id == field(usage, ^foreign_key),
        select: map(usage, ^[foreign_key, :last_used_at])
      )

    on_conflict =
      from(usage in RecordUsage,
        update: [
          set: [last_used_at: fragment("GREATEST(?, EXCLUDED.last_used_at)", usage.last_used_at)]
        ]
      )

    Repo.insert_all(RecordUsage, query,
      conflict_target: [foreign_key],
      on_conflict: on_conflict
    )

    :ok
  end

  @spec foreign_key(resource_type()) :: atom()
  defp foreign_key(OauthAccessToken), do: :oauth_access_token_id
end
