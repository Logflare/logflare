defmodule Logflare.Backends.SourceSupWorker do
  @moduledoc """
  Worker that performs periodic cleanup ensure that SourceSup procs are correctly pulled down when deleted.
  """
  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
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

    rules_backends =
      Backends.list_backends(rules_source_id: source.id, enabled: true)

    # start rules source-backends
    rules_backend_ids =
      for backend <- rules_backends, into: MapSet.new() do
        SourceSup.start_backend_child(source, %{backend | register_for_ingest: false})
        backend.id
      end

    # start attached source-backends
    for backend <- backends do
      SourceSup.start_backend_child(source, backend)
    end

    via = Backends.via_source(source, Backends.SourceSup)

    # stop stale rule source-backends
    attached_backend_ids = MapSet.new(backends, fn backend -> backend.id end)

    backend_ids = MapSet.union(rules_backend_ids, attached_backend_ids)

    for {{_mod, _source_id, backend_id}, _, _, _} <- Supervisor.which_children(via),
        backend_id not in backend_ids,
        backend_id do
      SourceSup.stop_backend_child(source, backend_id)
    end
  end
end
