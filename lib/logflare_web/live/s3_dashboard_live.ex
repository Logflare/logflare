defmodule LogflareWeb.Live.S3DashboardLive do
  @moduledoc false

  use LogflareWeb, :live_view

  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue

  @max_points 60
  @tick_ms 1_000
  @pipeline Logflare.Backends.S3ProducerPipeline

  @impl Phoenix.LiveView
  def mount(_params, _session, socket) do
    write_rate_atomic = :atomics.new(1, [])
    write_total_atomic = :atomics.new(1, [])
    read_rate_atomic = :atomics.new(1, [])
    read_total_atomic = :atomics.new(1, [])

    if connected?(socket) do
      pid = :erlang.pid_to_list(self())

      :telemetry.attach(
        "s3-dashboard-write-#{pid}",
        [:logflare, :backends, :pipeline, :handle_batch],
        fn _event, %{batch_size: size}, meta, {rate_ref, total_ref} ->
          if Map.get(meta, :backend_type) == :s3_producer do
            :atomics.add(rate_ref, 1, size)
            :atomics.add(total_ref, 1, size)
          end
        end,
        {write_rate_atomic, write_total_atomic}
      )

      :telemetry.attach(
        "s3-dashboard-read-#{pid}",
        [:logflare, :backends, :s3_consumer, :dispatch],
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
        Backends.s3_producer_mode?() and Backends.s3_consumer_mode?() -> "both"
        Backends.s3_producer_mode?() -> "producer"
        Backends.s3_consumer_mode?() -> "consumer"
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
       read_total_atomic: read_total_atomic
     )}
  end

  @impl Phoenix.LiveView
  def terminate(_reason, socket) do
    pid = :erlang.pid_to_list(self())
    :telemetry.detach("s3-dashboard-write-#{pid}")
    :telemetry.detach("s3-dashboard-read-#{pid}")
    socket
  end

  @impl Phoenix.LiveView
  def handle_info(:tick, socket) do
    Process.send_after(self(), :tick, @tick_ms)

    metrics =
      gather_metrics(
        socket.assigns.write_rate_atomic,
        socket.assigns.write_total_atomic,
        socket.assigns.read_rate_atomic,
        socket.assigns.read_total_atomic
      )
    label = Time.utc_now() |> Time.truncate(:second) |> Time.to_string()
    point = Map.put(metrics, :t, label)
    series = (socket.assigns.series ++ [point]) |> Enum.take(-@max_points)

    {:noreply, assign(socket, series: series, current: metrics)}
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
        "Components.S3Dashboard",
        %{
          data: @series,
          current: @current,
          producer_paused: @producer_paused,
          mode: @mode
        },
        id: "s3-dashboard"
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
          Pausing the producer suspends S3 writes — ETS queue will grow.
        </div>
      </div>
    </div>
    """
  end

  defp gather_metrics(write_rate_atomic, write_total_atomic, read_rate_atomic, read_total_atomic) do
    s3_key = {:s3_producer, nil}
    ets_pending = to_int(IngestEventQueue.total_by_status(s3_key, :pending))
    ets_processing = to_int(IngestEventQueue.total_by_status(s3_key, :processing))

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

  defp fetch_sqs_depth do
    s3_config = Application.get_env(:logflare, :s3_spool, [])
    provider = Keyword.get(s3_config, :provider, :aws)
    queue_name = Keyword.get(s3_config, :queue_name)

    cond do
      provider != :aws ->
        # Pub/Sub has no cheap message-count attribute; Cloud Monitoring would be needed.
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

  defp to_int(n) when is_integer(n), do: n
  defp to_int(_), do: 0

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
      total_mb: 0.0
    }
  end
end
