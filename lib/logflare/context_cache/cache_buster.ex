defmodule Logflare.ContextCache.CacheBuster do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """
  use GenServer

  alias Logflare.ContextCache.CacheBusterWorker
  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Auth
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.Endpoints
  alias Logflare.KeyValues
  alias Logflare.PubSub
  alias Logflare.Rules
  alias Logflare.SavedSearches
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.TeamUsers
  alias Logflare.Users
  alias Logflare.ContextCache.CacheBusterWorker

  require Logger

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    subscribe_to_transactions()
    {:ok, %{}}
  end

  def subscribe_to_transactions do
    Phoenix.PubSub.subscribe(PubSub, "wal_transactions")
  end

  @doc """
  Sets the Logger level for this process. It's started with level :error.

  To debug wal records set process to level :debug and each transaction will be logged.

  iex> Logflare.ContextCache.CacheBuster.set_log_level(:debug)
  """

  @spec set_log_level(Logger.level()) :: :ok
  def set_log_level(level) when is_atom(level) do
    GenServer.call(__MODULE__, {:put_level, level})
  end

  def handle_call({:put_level, level}, _from, state) do
    :ok = Logger.put_process_level(self(), level)

    {:reply, :ok, state}
  end

  def handle_info(%Transaction{changes: changes} = transaction, state) do
    Logger.debug("WAL record received from pubsub: #{inspect(transaction)}")

    for record <- changes,
        record = handle_record(record),
        record != :noop do
      record
    end
    |> tap(fn
      [] ->
        nil

      records ->
        :telemetry.execute([:logflare, :cache_buster, :to_bust], %{count: length(records)})

        CacheBusterWorker.cast_to_bust(records)
    end)

    {:noreply, state}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "sources"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Sources, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "rules"},
         record: record,
         old_record: old_record
       }) do
    {:refresh, {Rules, handle_rule_record(record) ++ handle_rule_record(old_record)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Users, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "plans"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {SourceSchemas, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "backends"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Backends, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {TeamUsers, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Auth, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:refresh, {Endpoints, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       }) do
    {:refresh, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Billing, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Endpoints, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       })
       when is_binary(source_id) do
    {:refresh, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {SourceSchemas, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "sources"},
         record: %{"id" => _id, "user_id" => user_id}
       })
       when is_binary(user_id) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Sources, :not_found}}
    # {:bust, {Users, String.to_integer(user_id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "rules"},
         record: record
       }) do
    {:refresh, {Rules, handle_rule_record(record)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "users"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Users, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "backends"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Backends, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {TeamUsers, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {:refresh, {Auth, :not_found}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       }) do
    {:refresh, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "billing_accounts"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "sources"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {Sources, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "endpoint_queries"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {Endpoints, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "source_schemas"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {SourceSchemas, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {Users, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "backends"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:bust, {Backends, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "rules"},
         old_record: record
       }) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {:bust, {Rules, handle_rule_record(record)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "team_users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {:bust, {TeamUsers, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "oauth_access_tokens"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {:bust, {Auth, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "saved_searches"},
         old_record: %{"source_id" => source_id}
       }) do
    {:bust, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "key_values"},
         record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {KeyValues, [user_id: String.to_integer(uid), key: key]}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "key_values"},
         record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {KeyValues, [user_id: String.to_integer(uid), key: key]}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "key_values"},
         old_record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {KeyValues, [user_id: String.to_integer(uid), key: key]}
  end

  defp handle_record(_record) do
    :noop
  end

  def handle_rule_record(%{
        "id" => <<id::binary>>,
        "source_id" => <<sid::binary>>,
        "backend_id" => <<bid::binary>>
      }) do
    [
      id: String.to_integer(id),
      source_id: String.to_integer(sid),
      backend_id: String.to_integer(bid)
    ]
  end

  def handle_rule_record(%{"id" => <<id::binary>>, "backend_id" => <<bid::binary>>}) do
    [id: String.to_integer(id), backend_id: String.to_integer(bid)]
  end

  def handle_rule_record(%{"id" => <<id::binary>>, "source_id" => <<sid::binary>>}) do
    [id: String.to_integer(id), source_id: String.to_integer(sid)]
  end

  def handle_rule_record(%{"id" => <<id::binary>>}) do
    [id: String.to_integer(id)]
  end
end
