defmodule Logflare.ContextCache.CacheBusterWorker do
  @moduledoc """
    Monitors our Postgres replication log and busts the cache accordingly.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.Rules
  alias Logflare.Utils

  @supervisor_name __MODULE__.Supervisor
  @reconcile_retry_delay 100
  @reconcile_retries 3

  @spec supervisor_spec() :: Supervisor.module_spec()
  def supervisor_spec do
    {PartitionSupervisor, child_spec: __MODULE__, name: @supervisor_name}
  end

  @spec cast_to_bust([{context, args}], [pos_integer()]) :: :ok
        when context: module(), args: term()
  def cast_to_bust(records, backend_owner_ids \\ []) do
    GenServer.cast(
      {:via, PartitionSupervisor, {@supervisor_name, records}},
      {:to_bust, records, backend_owner_ids}
    )
  end

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args)
  end

  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_cast({:to_bust, context_pkeys, backend_owner_ids}, state) do
    default_ingest_user_ids =
      context_pkeys
      |> backend_ids()
      |> Backends.cached_default_ingest_user_ids()
      |> Enum.concat(backend_owner_ids)
      |> Enum.uniq()

    {backend_records, cache_records} = Enum.split_with(context_pkeys, &backend_record?/1)

    bust_keys(cache_records)
    reconcile_backends(backend_records)
    refresh_default_ingest_caches(default_ingest_user_ids)
    Enum.each(cache_records, &maybe_do_cross_cluster_syncing/1)

    {:noreply, state}
  end

  @impl true
  def handle_info({:retry_backend_reconciliation, backend_id, retries_left}, state) do
    reconcile_backend(backend_id, retries_left)
    {:noreply, state}
  end

  defp refresh_default_ingest_caches([]), do: :ok

  defp refresh_default_ingest_caches(user_ids) do
    Utils.Tasks.start_child(fn -> Enum.each(user_ids, &refresh_default_ingest_cache/1) end)

    :ok
  end

  defp refresh_default_ingest_cache(user_id) do
    Backends.refresh_default_ingest_backends_cache(user_id)
  catch
    :exit, reason ->
      Logger.warning("Default ingest lookup refresh exited: #{inspect(reason)}",
        user_id: user_id
      )
  end

  defp backend_ids(context_pkeys) do
    for {Backends, backend_id} when is_integer(backend_id) <- context_pkeys, do: backend_id
  end

  @spec backend_record?({module(), term()}) :: boolean()
  defp backend_record?({Backends, backend_id}) when is_integer(backend_id), do: true
  defp backend_record?(_record), do: false

  @spec reconcile_backends([{Backends, integer()}]) :: :ok
  defp reconcile_backends(records) do
    Enum.each(records, fn {Backends, backend_id} ->
      reconcile_backend(backend_id, @reconcile_retries)
    end)
  end

  @spec reconcile_backend(integer(), non_neg_integer()) :: :ok
  defp reconcile_backend(backend_id, retries_left) do
    Backends.reconcile_backend_local(backend_id)
  catch
    kind, reason ->
      Logger.error(
        "Backend reconciliation failed: #{Exception.format(kind, reason, __STACKTRACE__)}",
        backend_id: backend_id
      )

      if retries_left > 0 do
        Process.send_after(
          self(),
          {:retry_backend_reconciliation, backend_id, retries_left - 1},
          @reconcile_retry_delay
        )
      else
        bust_keys([{Backends, backend_id}])
      end

      :ok
  end

  @spec bust_keys([{module(), term()}]) :: :ok
  defp bust_keys([]), do: :ok

  defp bust_keys(context_pkeys) do
    ContextCache.Gossip.record_tombstones(context_pkeys)
    ContextCache.bust_keys(context_pkeys)
    :ok
  end

  defp maybe_do_cross_cluster_syncing({Rules, rule_id}) when is_integer(rule_id) do
    # sync rule
    Utils.Tasks.start_child(fn ->
      Rules.sync_rule(rule_id)
    end)
  end

  defp maybe_do_cross_cluster_syncing(_), do: :noop
end
