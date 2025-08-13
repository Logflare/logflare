defmodule Logflare.ContextCache.CacheBuster do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.ContextCache
  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.UpdatedRecord
  alias Cainophile.Changes.DeletedRecord
  alias Cainophile.Changes.Transaction
  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.TeamUsers
  alias Logflare.Users
  alias Logflare.Rules
  alias Logflare.Utils
  alias Logflare.PubSub
  alias Logflare.Auth
  alias Logflare.Billing
  alias Logflare.Endpoints

  # worker process
  defmodule Worker do
    use GenServer
    alias Logflare.ContextCache

    def start_link(init_args) do
      GenServer.start_link(__MODULE__, init_args)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_cast({:to_bust, context_pkeys}, state) do
      ContextCache.bust_keys(context_pkeys)
      {:noreply, state}
    end
  end

  # main process
  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(_state) do
    subscribe_to_transactions()

    {:ok, _pid} =
      PartitionSupervisor.start_link(
        child_spec: __MODULE__.Worker,
        name: __MODULE__.Supervisor
      )

    {:ok, %{partitions: System.schedulers_online()}}
  end

  def subscribe_to_transactions do
    Phoenix.PubSub.subscribe(PubSub, "wal_transactions")
  end

  @doc """
  Sets the Logger level for this process. It's started with level :error.

  To debug wal records set process to level :debug and each transaction will be logged.

  iex> Logflare.ContextCache.CacheBuster.set_log_level(:debug)
  """

  @spec set_log_level(Logger.levels()) :: :ok
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
      maybe_do_cross_cluster_syncing(record)
      record
    end
    |> tap(fn
      [] ->
        nil

      records ->
        :telemetry.execute([:logflare, :cache_buster, :to_bust], %{count: length(records)})
    end)
    |> then(fn records ->
      GenServer.cast(
        {:via, PartitionSupervisor, {__MODULE__.Supervisor, records}},
        {:to_bust, records}
      )
    end)

    {:noreply, state}
  end

  defp maybe_do_cross_cluster_syncing({Alerting, alert_id}) do
    # sync alert job
    Utils.Tasks.start_child(fn ->
      Alerting.sync_alert_job(alert_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({Backends, backend_id})
       when is_integer(backend_id) do
    # sync backend across cluster for v2 sources
    Utils.Tasks.start_child(fn ->
      Backends.sync_backend_across_cluster(backend_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing({Rules, rule_id}) do
    # sync rule
    Utils.Tasks.start_child(fn ->
      Rules.sync_rule(rule_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing(_), do: :noop

  defp handle_record(%UpdatedRecord{
         relation: {_schema, "sources"},
         record: %{"id" => id}
       })
       when is_binary(id) do
    {Sources, String.to_integer(id)}
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
         record: %{"id" => _id, "source_id" => source_id}
       })
       when is_binary(source_id) do
    # When new records are created they were previously cached as `nil` so we need to bust the :not_found keys
    {Sources, String.to_integer(source_id)}
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
         old_record: %{"id" => _id, "source_id" => source_id}
       })
       when is_binary(source_id) do
    # Must do `alter table rules replica identity full` to get full records on deletes otherwise all fields are null
    {Sources, String.to_integer(source_id)}
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

  defp handle_record(_record) do
    :noop
  end
end
