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
    {Sources, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "rules"},
         record: record,
         old_record: old_record
       }) do
    {Rules, handle_rule_record(record) ++ handle_rule_record(old_record)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Users, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Billing, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "plans"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Billing, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {SourceSchemas, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "backends"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Backends, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {TeamUsers, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Auth, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Endpoints, String.to_integer(id)}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       }) do
    {SavedSearches, [source_id: String.to_integer(source_id)]}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Billing, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Endpoints, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {SourceSchemas, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "sources"},
         record: %{"id" => _id, "user_id" => user_id}
       })
       when is_binary(user_id) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Sources, :not_found}
    # {Users, String.to_integer(user_id)}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "rules"},
         record: record
       }) do
    {Rules, handle_rule_record(record)}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "users"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Users, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "backends"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Backends, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {TeamUsers, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => _id}
       }) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Auth, :not_found}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       }) do
    {SavedSearches, [source_id: String.to_integer(source_id)]}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "billing_accounts"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Billing, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "sources"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Sources, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "endpoint_queries"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Endpoints, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "source_schemas"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {SourceSchemas, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Users, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "backends"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {Backends, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "rules"},
         old_record: record
       }) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {Rules, handle_rule_record(record)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "team_users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {TeamUsers, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "oauth_access_tokens"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {Auth, String.to_integer(id)}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "saved_searches"},
         old_record: %{"source_id" => source_id}
       }) do
    {SavedSearches, [source_id: String.to_integer(source_id)]}
  end

  defp handle_record(_record) do
    :noop
  end

  defp handle_rule_record(record) do
    for {k, v} <- Map.take(record, ["source_id", "backend_id"]), is_binary(v) do
      {String.to_existing_atom(k), String.to_integer(v)}
    end
  end
end
