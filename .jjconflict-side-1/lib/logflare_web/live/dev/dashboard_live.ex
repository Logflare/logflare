defmodule LogflareWeb.Live.Dev.DashboardLive do
  @moduledoc false

  use LogflareWeb, :live_view

  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  @max_points 1_800
  @tick_ms 1_000
  @pipeline Logflare.Backends.Spool.ProducerPipeline

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    write_rate_atomic = :atomics.new(1, [])
    write_total_atomic = :atomics.new(1, [])
    read_rate_atomic = :atomics.new(1, [])
    read_total_atomic = :atomics.new(1, [])
    ch_rate_atomic = :atomics.new(1, [])
    ch_total_atomic = :atomics.new(1, [])
    bq_rate_atomic = :atomics.new(1, [])
    bq_total_atomic = :atomics.new(1, [])

    if connected?(socket) do
      pid = :erlang.pid_to_list(self())

      :telemetry.attach(
        "dev-dashboard-write-#{pid}",
        [:logflare, :backends, :pipeline, :handle_batch],
        fn _event, %{batch_size: size}, meta, refs ->
          accumulate_batch(meta, size, :spool_producer, refs)
        end,
        {write_rate_atomic, write_total_atomic}
      )

      :telemetry.attach(
        "dev-dashboard-ch-#{pid}",
        [:logflare, :backends, :pipeline, :handle_batch],
        fn _event, %{batch_size: size}, meta, refs ->
          accumulate_batch(meta, size, :clickhouse, refs)
        end,
        {ch_rate_atomic, ch_total_atomic}
      )

      :telemetry.attach(
        "dev-dashboard-bq-#{pid}",
        [:logflare, :backends, :pipeline, :handle_batch],
        fn _event, %{batch_size: size}, meta, refs ->
          accumulate_batch(meta, size, :bigquery, refs)
        end,
        {bq_rate_atomic, bq_total_atomic}
      )

      :telemetry.attach(
        "dev-dashboard-read-#{pid}",
        [:logflare, :backends, :spool_consumer, :dispatch],
        fn _event, %{count: count}, _meta, {rate_ref, total_ref} ->
          :atomics.add(rate_ref, 1, count)
          :atomics.add(total_ref, 1, count)
        end,
        {read_rate_atomic, read_total_atomic}
      )

      Process.send_after(self(), :tick, @tick_ms)
    end

    mode =
      cond do
        Backends.spool_producer_mode?() and Backends.spool_consumer_mode?() -> "both"
        Backends.spool_producer_mode?() -> "producer"
        Backends.spool_consumer_mode?() -> "consumer"
        true -> "none"
      end

    {:ok,
     assign(socket,
       series: [],
       current: empty_metrics(),
       producer_paused: false,
       mode: mode,
       write_rate_atomic: write_rate_atomic,
       write_total_atomic: write_total_atomic,
       read_rate_atomic: read_rate_atomic,
       read_total_atomic: read_total_atomic,
       ch_rate_atomic: ch_rate_atomic,
       ch_total_atomic: ch_total_atomic,
       bq_rate_atomic: bq_rate_atomic,
       bq_total_atomic: bq_total_atomic,
       prev_minor_gc: elem(:erlang.statistics(:garbage_collection), 0),
       prev_words_reclaimed: elem(:erlang.statistics(:garbage_collection), 1),
       prev_scheduler_wall_time: sample_scheduler_wall_time()
     )}
  end

  @impl Phoenix.LiveView
  def terminate(_reason, socket) do
    pid = :erlang.pid_to_list(self())
    :telemetry.detach("dev-dashboard-write-#{pid}")
    :telemetry.detach("dev-dashboard-ch-#{pid}")
    :telemetry.detach("dev-dashboard-bq-#{pid}")
    :telemetry.detach("dev-dashboard-read-#{pid}")
    socket
  end

  @impl Phoenix.LiveView
  def handle_info(:tick, socket) do
    Process.send_after(self(), :tick, @tick_ms)

    {ch_metrics, prev_minor_gc, prev_words_reclaimed} =
      gather_ch_metrics(
        socket.assigns.ch_rate_atomic,
        socket.assigns.ch_total_atomic,
        socket.assigns.bq_rate_atomic,
        socket.assigns.bq_total_atomic,
        socket.assigns.prev_minor_gc,
        socket.assigns.prev_words_reclaimed
      )

    {cpu_metrics, prev_scheduler_wall_time} =
      gather_cpu_metrics(socket.assigns.prev_scheduler_wall_time)

    metrics =
      gather_metrics(
        socket.assigns.write_rate_atomic,
        socket.assigns.write_total_atomic,
        socket.assigns.read_rate_atomic,
        socket.assigns.read_total_atomic
      )
      |> Map.merge(ch_metrics)
      |> Map.merge(cpu_metrics)

    label = Time.utc_now() |> Time.truncate(:second) |> Time.to_string()

    point =
      metrics
      |> Map.put(:t, label)
      |> Map.put(:ts, System.system_time(:millisecond))

    series = (socket.assigns.series ++ [point]) |> Enum.take(-@max_points)

    {:noreply,
     assign(socket,
       series: series,
       current: metrics,
       prev_minor_gc: prev_minor_gc,
       prev_words_reclaimed: prev_words_reclaimed,
       prev_scheduler_wall_time: prev_scheduler_wall_time
     )}
  end

  @impl Phoenix.LiveView
  def handle_event("toggle_producer", _params, socket) do
    paused = socket.assigns.producer_paused

    result =
      try do
        producers = Broadway.producer_names(@pipeline)

        if paused do
          Enum.each(producers, &:sys.resume/1)
        else
          Enum.each(producers, &:sys.suspend/1)
        end

        {:ok, !paused}
      rescue
        _ -> {:error, paused}
      end

    case result do
      {:ok, new_state} -> {:noreply, assign(socket, producer_paused: new_state)}
      {:error, _} -> {:noreply, socket}
    end
  end

  @impl Phoenix.LiveView
  def render(assigns) do
    ~H"""
    <div style="background: #181825; min-height: 100vh; padding: 0;">
      {live_react_component(
        "Components.DevDashboard",
        %{
          data: @series,
          current: @current,
          producer_paused: @producer_paused,
          mode: @mode
        },
        id: "dev-dashboard"
      )}

      <div style="padding: 0 24px 24px; display: flex; gap: 12px;">
        <button
          phx-click="toggle_producer"
          style={[
            "padding: 10px 24px; border-radius: 8px; border: none; cursor: pointer; font-family: monospace; font-size: 13px; font-weight: 600; transition: all 0.15s;",
            if(@producer_paused,
              do: "background: #a6e3a1; color: #1e1e2e;",
              else: "background: #f38ba8; color: #1e1e2e;"
            )
          ]}
        >
          {if @producer_paused, do: "▶ Resume Producer", else: "⏸ Pause Producer"}
        </button>

        <div style="color: #6c7086; font-size: 12px; align-self: center; font-family: monospace;">
          Pausing the producer suspends spool writes — ETS queue will grow.
        </div>
      </div>
    </div>
    """
  end

  defp gather_metrics(write_rate_atomic, write_total_atomic, read_rate_atomic, read_total_atomic) do
    spool_key = {:spool_producer, nil}
    ets_pending = IngestEventQueue.total_by_status(spool_key, :pending)
    ets_processing = IngestEventQueue.total_by_status(spool_key, :processing)

    ets_bytes = :erlang.memory(:ets)
    proc_bytes = :erlang.memory(:processes)
    total_bytes = :erlang.memory(:total)

    {sqs_visible, sqs_inflight} = fetch_sqs_depth()

    write_rate = :atomics.exchange(write_rate_atomic, 1, 0)
    written_total = :atomics.get(write_total_atomic, 1)
    read_rate = :atomics.exchange(read_rate_atomic, 1, 0)
    read_total = :atomics.get(read_total_atomic, 1)

    %{
      ets_pending: ets_pending,
      ets_processing: ets_processing,
      sqs_visible: sqs_visible,
      sqs_inflight: sqs_inflight,
      write_rate: write_rate,
      written_total: written_total,
      read_rate: read_rate,
      read_total: read_total,
      ets_mb: Float.round(ets_bytes / 1_048_576, 1),
      proc_mb: Float.round(proc_bytes / 1_048_576, 1),
      total_mb: Float.round(total_bytes / 1_048_576, 1)
    }
  end

  # SQS-specific: Pub/Sub has no cheap message-count attribute (Cloud Monitoring
  # would be needed), so this stays zero for the GCP provider.
  defp fetch_sqs_depth do
    spool_config = Application.get_env(:logflare, :spool, [])
    provider = Keyword.get(spool_config, :provider, :aws)
    queue_name = Keyword.get(spool_config, :queue_name)

    cond do
      provider != :aws ->
        {0, 0}

      is_nil(queue_name) ->
        {0, 0}

      true ->
        sqs_cfg = Application.get_env(:ex_aws, :sqs, [])
        scheme = Keyword.get(sqs_cfg, :scheme, "https://")
        host = Keyword.get(sqs_cfg, :host, "sqs.us-east-1.amazonaws.com")
        port = Keyword.get(sqs_cfg, :port)
        base = if port, do: "#{scheme}#{host}:#{port}", else: "#{scheme}#{host}"
        queue_url = "#{base}/000000000000/#{queue_name}"

        attrs = [:approximate_number_of_messages, :approximate_number_of_messages_not_visible]

        case ExAws.SQS.get_queue_attributes(queue_url, attrs) |> ExAws.request() do
          {:ok, %{body: %{attributes: attrs}}} ->
            visible = Map.get(attrs, :approximate_number_of_messages, 0)
            inflight = Map.get(attrs, :approximate_number_of_messages_not_visible, 0)
            {visible, inflight}

          _ ->
            {0, 0}
        end
    end
  end

  defp gather_ch_metrics(
         ch_rate_atomic,
         ch_total_atomic,
         bq_rate_atomic,
         bq_total_atomic,
         prev_minor_gc,
         prev_words_reclaimed
       ) do
    ch_batch_rate = :atomics.exchange(ch_rate_atomic, 1, 0)
    ch_total = :atomics.get(ch_total_atomic, 1)
    bq_batch_rate = :atomics.exchange(bq_rate_atomic, 1, 0)
    bq_total = :atomics.get(bq_total_atomic, 1)

    ch_mem = sum_process_memory(ch_pipeline_pids())
    bq_mem = sum_process_memory(bq_pipeline_pids())

    {total_minor_gc, total_words_reclaimed, _} = :erlang.statistics(:garbage_collection)
    gc_minor_rate = max(0, total_minor_gc - prev_minor_gc)

    gc_reclaimed_mb =
      Float.round(max(0, total_words_reclaimed - prev_words_reclaimed) * 8 / 1_048_576, 1)

    metrics = %{
      ch_batch_rate: ch_batch_rate,
      ch_total: ch_total,
      bq_batch_rate: bq_batch_rate,
      bq_total: bq_total,
      ch_proc_mb: Float.round(ch_mem / 1_048_576, 1),
      bq_proc_mb: Float.round(bq_mem / 1_048_576, 1),
      gc_minor_rate: gc_minor_rate,
      gc_major_rate: 0,
      gc_long_rate: 0,
      gc_reclaimed_mb: gc_reclaimed_mb
    }

    {metrics, total_minor_gc, total_words_reclaimed}
  end

  defp gather_cpu_metrics(prev_wall_time) do
    os_cpu = Float.round(:cpu_sup.util() * 1.0, 1)
    new_wall_time = sample_scheduler_wall_time()

    scheduler_pct =
      prev_wall_time
      |> Enum.zip(new_wall_time)
      |> Enum.map(fn {{_, prev_active, prev_total}, {_, new_active, new_total}} ->
        active_delta = new_active - prev_active
        total_delta = new_total - prev_total
        if total_delta > 0, do: active_delta / total_delta, else: 0.0
      end)
      |> then(fn utils ->
        if utils == [], do: 0.0, else: Enum.sum(utils) / length(utils) * 100
      end)
      |> Float.round(1)

    {%{os_cpu: os_cpu, scheduler_pct: scheduler_pct}, new_wall_time}
  end

  defp sample_scheduler_wall_time do
    :erlang.system_flag(:scheduler_wall_time, true)
    :erlang.statistics(:scheduler_wall_time) |> Enum.sort()
  end

  defp pipeline_pids(ancestor) do
    Process.list()
    |> Enum.filter(fn pid ->
      case Process.info(pid, :dictionary) do
        {:dictionary, dict} ->
          Enum.member?(Keyword.get(dict, :"$ancestors", []), ancestor)

        _ ->
          false
      end
    end)
  end

  defp ch_pipeline_pids, do: pipeline_pids(Logflare.Backends.ConsolidatedSup)
  defp bq_pipeline_pids, do: pipeline_pids(Logflare.Backends.SourcesSup)

  defp sum_process_memory(pids) do
    Enum.reduce(pids, 0, fn pid, acc ->
      case Process.info(pid, :memory) do
        {:memory, mem} -> acc + mem
        nil -> acc
      end
    end)
  end

  defp accumulate_batch(meta, size, backend_type, {rate_ref, total_ref}) do
    if Map.get(meta, :backend_type) == backend_type do
      :atomics.add(rate_ref, 1, size)
      :atomics.add(total_ref, 1, size)
    end
  end

  defp empty_metrics do
    %{
      ets_pending: 0,
      ets_processing: 0,
      sqs_visible: 0,
      sqs_inflight: 0,
      write_rate: 0,
      written_total: 0,
      read_rate: 0,
      read_total: 0,
      ets_mb: 0.0,
      proc_mb: 0.0,
      total_mb: 0.0,
      ch_batch_rate: 0,
      ch_total: 0,
      bq_batch_rate: 0,
      bq_total: 0,
      ch_proc_mb: 0.0,
      bq_proc_mb: 0.0,
      gc_minor_rate: 0,
      gc_major_rate: 0,
      gc_long_rate: 0,
      gc_reclaimed_mb: 0.0,
      os_cpu: 0.0,
      scheduler_pct: 0.0
    }
  end
end
