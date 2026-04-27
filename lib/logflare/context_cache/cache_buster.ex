defmodule Logflare.ContextCache.CacheBuster do
  @moduledoc """
  Monitors our Postgres replication log and busts/updates the cache accordingly.

  TransactionBroadcaster calls `broadcast_cache_updates/1` with raw WAL changes.
  This module classifies them, calls `c:Logflare.ContextCache.bust_actions/2` on the relevant
  cache module, and broadcasts the result map cluster-wide via PubSub. Each node's CacheBuster
  GenServer receives the broadcast and applies updates to local caches without further DB queries.

  Modules without `bust_actions/2` fall through to a `{:partial, %{}}` plan that performs an
  ETS scan by integer pkey.
  """
  use GenServer

  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.UpdatedRecord
  alias Logflare.Auth
  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.Endpoints
  alias Logflare.KeyValues
  alias Logflare.PubSub
  alias Logflare.Rules
  alias Logflare.SavedSearches
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.TeamUsers
  alias Logflare.Users

  require Logger

  @pubsub_topic "cache_updates"

  def subscribe_updates do
    Phoenix.PubSub.subscribe(PubSub, @pubsub_topic)
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    subscribe_updates()
    {:ok, %{}}
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

  @type cainophile_change ::
          %NewRecord{}
          | %UpdatedRecord{}
          | %DeletedRecord{}
          | %Cainophile.Changes.TruncatedRelation{}

  @doc """
  Classifies WAL changes, calls `bust_actions/2` on each relevant cache module, and broadcasts
  the results cluster-wide.
  """
  @spec broadcast_cache_updates([cainophile_change()]) :: :ok
  def broadcast_cache_updates(changes) do
    results = classify_changes(changes)

    if results != [] do
      :telemetry.execute([:logflare, :cache_buster, :to_bust], %{count: length(results)})

      Phoenix.PubSub.broadcast(
        PubSub,
        @pubsub_topic,
        {:cache_updates, results}
      )
    end

    :ok
  end

  def handle_info({:cache_updates, results}, state) do
    CacheBusterWorker.cast_apply(results)
    {:noreply, state}
  end

  defp classify_changes(changes) do
    for record <- changes,
        record = handle_record(record),
        record != :noop do
      map_to_update_plan(record)
    end
  end

  defp map_to_update_plan({action, {context, trigger}}) do
    cache_module = ContextCache.cache_name(context)

    plan =
      if function_exported?(cache_module, :bust_actions, 2) do
        cache_module.bust_actions(action, trigger)
      else
        {:partial, %{}}
      end

    {context, trigger, plan}
  end

  # WAL record classification

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "sources"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Sources, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "rules"},
         record: record,
         old_record: old_record
       }) do
    {:update, {Rules, handle_rule_record(record) ++ handle_rule_record(old_record)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Users, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "plans"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    # TODO: This passes plan id as billing account.
    # ETS scan will bust it, but will bust matching billing accounts as well
    # Proper solution requires migrating Billing to keyword busting
    {:delete, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {SourceSchemas, id: String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "backends"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Backends, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {TeamUsers, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Auth, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Endpoints, String.to_integer(id)}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       }) do
    {:update, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "billing_accounts"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "endpoint_queries"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Endpoints, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "saved_searches"},
         record: %{"source_id" => source_id}
       })
       when is_binary(source_id) do
    {:update, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "source_schemas"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {SourceSchemas, id: String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "sources"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Sources, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "rules"},
         record: record
       }) do
    {:update, {Rules, handle_rule_record(record)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Users, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "backends"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Backends, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "team_users"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {TeamUsers, String.to_integer(id)}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "oauth_access_tokens"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {:update, {Auth, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "billing_accounts"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Billing, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "sources"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Sources, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "endpoint_queries"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Endpoints, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "source_schemas"},
         old_record: %{"id" => id, "source_id" => source_id}
       })
       when is_binary(id) and is_binary(source_id) do
    {:delete,
     {SourceSchemas, [id: String.to_integer(id), source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Users, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "backends"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Backends, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "rules"},
         old_record: record
       }) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {:delete, {Rules, handle_rule_record(record)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "team_users"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {TeamUsers, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "oauth_access_tokens"},
         old_record: %{"id" => id}
       })
       when is_binary(id) do
    {:delete, {Auth, String.to_integer(id)}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "saved_searches"},
         old_record: %{"source_id" => source_id}
       }) do
    {:delete, {SavedSearches, [source_id: String.to_integer(source_id)]}}
  end

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "key_values"},
         record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {:delete, {KeyValues, [user_id: String.to_integer(uid), key: key]}}
  end

  defp handle_record(%NewRecord{
         relation: {_schema, "key_values"},
         record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {:delete, {KeyValues, [user_id: String.to_integer(uid), key: key]}}
  end

  defp handle_record(%DeletedRecord{
         relation: {_schema, "key_values"},
         old_record: %{"user_id" => uid, "key" => key}
       })
       when is_binary(uid) and is_binary(key) do
    {:delete, {KeyValues, [user_id: String.to_integer(uid), key: key]}}
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
