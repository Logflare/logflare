defmodule Logflare.Backends.SourceSupWorker do
  @moduledoc """
  Worker that performs periodic cleanup ensure that SourceSup procs are correctly pulled down when deleted.
  """
  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Rules
  alias Logflare.Sources

  @default_interval :timer.minutes(10)

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    source = opts[:source]
    state = %{source_id: source.id, interval: Keyword.get(opts, :interval, @default_interval)}
    Process.send_after(self(), :check, state.interval)
    {:ok, state}
  end

  def handle_info(:check, state) do
    source = Sources.Cache.get_by_id(state.source_id)
    do_check(source)
    Process.send_after(self(), :check, state.interval)
    {:noreply, state}
  end

  defp do_check(nil), do: :noop

  defp do_check(source) do
    backends = Backends.list_backends(source_id: source.id, enabled: true)

    # This could use one Backends.list_backends(rules_source_id: source.id, enabled: true)
    # query and start each backend with register_for_ingest: false; keeping the rule helpers
    # makes the lifecycle intent clearer.
    rules =
      source
      |> Rules.list_rules_with_backend()
      |> Logflare.Repo.preload(:backend)

    rules_backend_ids =
      for %{backend: %{enabled: true} = backend} = rule <- rules,
          not Adaptor.consolidated_ingest?(backend),
          into: MapSet.new() do
        rule.backend_id
      end

    attached_backend_ids =
      backends
      |> Enum.reject(&Adaptor.consolidated_ingest?/1)
      |> MapSet.new(& &1.id)

    desired_backend_ids = MapSet.union(rules_backend_ids, attached_backend_ids)

    via = Backends.via_source(source, Backends.SourceSup)

    running_backend_ids =
      for {{_mod, _source_id, backend_id}, _, _, _} <- Supervisor.which_children(via),
          is_integer(backend_id),
          into: MapSet.new(),
          do: backend_id

    desired_backend_ids
    |> MapSet.symmetric_difference(running_backend_ids)
    |> Enum.each(&reconcile_source_backend(source.id, &1))
  end

  @spec reconcile_source_backend(pos_integer(), pos_integer()) :: :ok
  defp reconcile_source_backend(source_id, backend_id) do
    Backends.reconcile_source_backend_local(source_id, backend_id)
  catch
    kind, reason ->
      Logger.warning(
        "Failed to reconcile source backend: #{Exception.format(kind, reason, __STACKTRACE__)}",
        source_id: source_id,
        backend_id: backend_id
      )

      :ok
  end
end
