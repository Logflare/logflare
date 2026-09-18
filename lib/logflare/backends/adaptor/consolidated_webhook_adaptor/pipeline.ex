defmodule Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.Pipeline do
  @moduledoc """
  Broadway pipeline for the consolidated webhook adaptor.

  The pipeline takes events from the consolidated queue of the backend and sends them
  as HTTP POST batches.

  Uses ID-passing: the producer emits `LogEventPointer`s while full events live in the
  generation store (see `Logflare.Backends.IngestEventQueue`). A processor resolves one
  event and encodes its body to JSON one time. The batch processor only joins the
  encoded bodies and sends the request.

  A failed request is requeued when the failure is transient. All other failures drop
  the affected events. Each transient failure counts toward the circuit breaker of the
  backend. The pipeline drops retries while the breaker is open.

  An event can wait in the queue until its generation is dropped. The processor then
  finds no event for the pointer. The pipeline counts these pointers in the
  `missing_ids` telemetry.
  """

  @behaviour Broadway.Acknowledger

  require Logger

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor
  alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.EncodedEvent
  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.CircuitBreaker
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.LogEvent
  alias Logflare.Utils

  @min_processor_concurrency 6
  @batcher_concurrency 4
  @batch_timeout if Application.compile_env(:logflare, :env) == :test, do: 10, else: 1_000
  @producer_interval if Application.compile_env(:logflare, :env) == :test, do: 10, else: 1_000
  @max_in_flight_batches 16
  @max_retries 1
  @retriable_statuses [408, 429]
  @content_types %{"json" => "application/json", "ndjson" => "application/x-ndjson"}
  @drop_log_interval_ms 5_000

  @typep drop_reason ::
           :retries_exhausted | :rejected | :queue_unavailable | :circuit_breaker_open

  @doc false
  @spec max_retries() :: non_neg_integer()
  def max_retries, do: @max_retries

  @doc false
  @spec processor_concurrency() :: pos_integer()
  def processor_concurrency, do: processor_concurrency(System.schedulers_online())

  # Same split as the ClickHouse consolidated pipeline: every scheduler the batch
  # processors do not reserve, with a floor that oversubscribes a small host on purpose.
  # This concurrency is allocated per backend, so the total grows with the number of
  # active backends. A throughput benchmark on ten schedulers measured the previous
  # fixed two processors as the limit of the whole pipeline, roughly 56% below what
  # four reach, with no further gain past that.
  @doc false
  @spec processor_concurrency(pos_integer()) :: pos_integer()
  def processor_concurrency(schedulers_online)
      when is_integer(schedulers_online) and schedulers_online > 0 do
    max(schedulers_online - @batcher_concurrency, @min_processor_concurrency)
  end

  @doc false
  @spec child_spec(arg :: term()) :: Supervisor.child_spec()
  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(keyword()) ::
          {:ok, pid()} | :ignore | {:error, {:already_started, pid()} | term()}
  def start_link(args) do
    {name, args} = Keyword.pop(args, :name)
    backend = Keyword.fetch!(args, :backend)
    batch_size = batch_size(backend)

    Broadway.start_link(__MODULE__,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10],
      producer: [
        module:
          {BufferProducer,
           [
             backend_id: backend.id,
             consolidated: true,
             id_passing: true,
             interval: @producer_interval,
             max_in_flight: batch_size * @max_in_flight_batches
           ]},
        transformer: {__MODULE__, :transform, [backend_id: backend.id]},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: processor_concurrency(), min_demand: 1]
      ],
      batchers: [
        http: [
          concurrency: @batcher_concurrency,
          batch_size: batch_size,
          batch_timeout: @batch_timeout
        ]
      ],
      context: %{
        backend_id: backend.id,
        backend_token: backend.token,
        user_id: backend.user_id
      }
    )
  end

  @doc """
  Batch size from the stored config of the backend.

  `Backends.create_backend/2` and `Backends.update_backend/2` start the pipeline before
  they typecast the config. At that point `config` is nil or holds the old values, and
  `config_encrypted` holds the new ones.
  """
  @spec batch_size(Backend.t()) :: pos_integer()
  def batch_size(%Backend{config_encrypted: stored, config: config}) do
    (stored || config || %{})
    |> ConsolidatedWebhookAdaptor.cast_config()
    |> Ecto.Changeset.get_field(:batch_size)
  end

  @spec process_name(via_tuple :: {:via, module(), {module(), term()}}, base_name :: term()) ::
          {:via, module(), {module(), term()}}
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, Utils.append_to_tuple(identifier, base_name)}}
  end

  @spec transform(pointer :: LogEventPointer.t(), opts :: keyword()) :: Message.t()
  def transform(%LogEventPointer{} = pointer, opts) do
    ack_data = %{
      backend_id: opts[:backend_id],
      in_flight_ref: BufferProducer.get_in_flight_ref(self())
    }

    %Message{data: pointer, acknowledger: {__MODULE__, :ack_id, ack_data}}
  end

  @spec handle_message(processor_name :: atom(), message :: Message.t(), context :: map()) ::
          Message.t()
  def handle_message(_processor_name, %Message{data: %LogEventPointer{} = pointer} = message, _) do
    message = Message.put_batcher(message, :http)

    case IngestEventQueue.lookup_event(pointer.tid, pointer.gen_event_id) do
      %LogEvent{body: body} -> encode_event(message, pointer, body)
      %EncodedEvent{} = encoded -> %{message | data: %{encoded | pointer: pointer}}
      nil -> Message.failed(message, :not_found)
    end
  end

  def handle_message(_processor_name, message, _context) do
    Message.failed(message, :not_found)
  end

  @spec encode_event(Message.t(), LogEventPointer.t(), map()) :: Message.t()
  defp encode_event(message, pointer, body) do
    case Jason.encode(body) do
      {:ok, json} ->
        encoded = %EncodedEvent{pointer: pointer, json: json}
        IngestEventQueue.replace_event(pointer.tid, pointer.gen_event_id, encoded)
        %{message | data: encoded}

      {:error, _reason} ->
        Message.failed(message, {:rejected, :json_encode})
    end
  end

  @spec handle_batch(
          batcher :: atom(),
          messages :: [Message.t()],
          batch_info :: Broadway.BatchInfo.t(),
          context :: map()
        ) :: [Message.t()]
  def handle_batch(:http, messages, batch_info, %{backend_id: backend_id} = context) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{backend_type: :consolidated_webhook, backend_id: backend_id}
    )

    case send_batch(Backends.Cache.get_backend(backend_id), messages, context) do
      :ok -> messages
      {:error, reason} -> Enum.map(messages, &Message.failed(&1, reason))
    end
  end

  @spec send_batch(Backend.t() | nil, [Message.t()], map()) ::
          :ok | {:error, {:retriable | :rejected, term()}}
  defp send_batch(nil, _messages, _context), do: {:error, {:rejected, :backend_not_found}}

  defp send_batch(%Backend{} = backend, messages, context) do
    config = backend |> Adaptor.get_backend_config() |> put_content_type()
    encoded_events = for %Message{data: %EncodedEvent{json: json}} <- messages, do: json

    [
      url: config.url,
      body: join_payload(config, encoded_events),
      headers: config.headers,
      gzip: Map.get(config, :gzip, true),
      http: Map.get(config, :http),
      opts: [metadata: egress_metadata(backend, context)]
    ]
    |> Client.send()
    |> classify_response()
    |> record_failure(backend)
  end

  @spec record_failure(result, Backend.t()) :: result
        when result: :ok | {:error, {:retriable | :rejected, term()}}
  defp record_failure({:error, {:retriable, _reason}} = result, backend) do
    CircuitBreaker.record_failure(backend)
    result
  end

  defp record_failure(result, _backend), do: result

  @doc """
  Joins JSON-encoded event bodies into one request body for the format of the config.
  """
  @spec join_payload(map(), [binary()]) :: binary()
  def join_payload(%{format: "ndjson"}, encoded_events) do
    encoded_events |> Enum.intersperse("\n") |> IO.iodata_to_binary()
  end

  def join_payload(_config, encoded_events) do
    IO.iodata_to_binary(["[", Enum.intersperse(encoded_events, ","), "]"])
  end

  @doc """
  Adds the content type of the payload format to the headers of the config.

  The request body is a binary, so the HTTP client does not set a content type. A
  `content-type` header from the user takes precedence.
  """
  @spec put_content_type(map()) :: map()
  def put_content_type(config) do
    content_type = Map.get(@content_types, Map.get(config, :format), @content_types["json"])

    headers =
      (Map.get(config, :headers) || %{})
      |> Headers.normalize_keys()
      |> Map.put_new("content-type", content_type)

    Map.put(config, :headers, headers)
  end

  @spec egress_metadata(Backend.t(), map()) :: map()
  defp egress_metadata(%Backend{metadata: backend_metadata}, context) do
    backend_meta = Map.new(backend_metadata || %{}, fn {k, v} -> {"backend.#{k}", v} end)

    Map.merge(
      %{
        "backend_id" => context.backend_id,
        "backend_uuid" => context.backend_token,
        "user_id" => context.user_id
      },
      backend_meta
    )
  end

  @spec classify_response({:ok, Tesla.Env.t()} | {:error, term()}) ::
          :ok | {:error, {:retriable | :rejected, term()}}
  defp classify_response({:ok, %Tesla.Env{status: status}}) when status in 200..299, do: :ok

  defp classify_response({:ok, %Tesla.Env{status: status}})
       when status in @retriable_statuses or status >= 500,
       do: {:error, {:retriable, status}}

  defp classify_response({:ok, %Tesla.Env{status: status}}), do: {:error, {:rejected, status}}
  defp classify_response({:error, reason}), do: {:error, {:retriable, reason}}

  @spec ack(ack_ref :: term(), successful :: [Message.t()], failed :: [Message.t()]) :: :ok
  def ack(_ack_ref, successful, failed) do
    release_in_flight(successful ++ failed)
    Enum.each(successful, &delete_payload(&1.data))

    failed
    |> Enum.group_by(&ack_backend_id/1)
    |> Enum.each(fn {backend_id, messages} -> handle_failed(backend_id, messages) end)
  end

  @spec release_in_flight([Message.t()]) :: :ok
  defp release_in_flight(messages) do
    messages
    |> Enum.frequencies_by(fn %Message{acknowledger: {_, _, ack_data}} ->
      Map.get(ack_data, :in_flight_ref)
    end)
    |> Enum.each(fn
      {nil, _count} -> :ok
      {ref, count} -> :atomics.sub(ref, 1, count)
    end)
  end

  @spec ack_backend_id(Message.t()) :: pos_integer()
  defp ack_backend_id(%Message{acknowledger: {_, _, %{backend_id: backend_id}}}), do: backend_id

  @spec handle_failed(pos_integer(), [Message.t()]) :: :ok
  defp handle_failed(backend_id, messages) do
    messages
    |> Enum.group_by(&failure_action/1, & &1.data)
    |> Enum.each(fn
      {:requeue, payloads} -> requeue_or_shed(backend_id, payloads)
      {:not_found, payloads} -> emit_missing_ids(backend_id, length(payloads))
      {reason, payloads} -> drop(backend_id, payloads, reason)
    end)
  end

  @spec failure_action(Message.t()) :: :requeue | :not_found | drop_reason()
  defp failure_action(%Message{status: {:failed, :not_found}}), do: :not_found

  defp failure_action(%Message{status: {:failed, {:retriable, _reason}}, data: data}) do
    if pointer(data).retries < @max_retries, do: :requeue, else: :retries_exhausted
  end

  defp failure_action(_message), do: :rejected

  @spec requeue_or_shed(pos_integer(), [EncodedEvent.t()]) :: :ok
  defp requeue_or_shed(backend_id, encoded_events) do
    case CircuitBreaker.check(backend_id) do
      :ok ->
        requeue(backend_id, encoded_events)

      {:error, :circuit_open, _blocked_until} ->
        drop(backend_id, encoded_events, :circuit_breaker_open)
    end
  end

  @spec requeue(pos_integer(), [EncodedEvent.t()]) :: :ok
  defp requeue(backend_id, encoded_events) do
    Logger.info("Requeuing #{length(encoded_events)} webhook events for retry",
      backend_id: backend_id
    )

    results = Enum.frequencies_by(encoded_events, &requeue_encoded(backend_id, &1))

    emit_dropped(backend_id, Map.get(results, :queue_unavailable, 0), :queue_unavailable)
  end

  @spec requeue_encoded(pos_integer(), EncodedEvent.t()) :: :requeued | :queue_unavailable
  defp requeue_encoded(backend_id, %EncodedEvent{pointer: pointer} = encoded) do
    retried = %{pointer | retries: pointer.retries + 1}

    case IngestEventQueue.requeue_payload(
           {:consolidated, backend_id},
           retried,
           &%{encoded | pointer: &1}
         ) do
      {:error, :not_initialized} -> :queue_unavailable
      _published_or_deduplicated -> :requeued
    end
  end

  @spec emit_missing_ids(pos_integer(), pos_integer()) :: :ok
  defp emit_missing_ids(backend_id, count) do
    :telemetry.execute(
      [:logflare, :ingest_event_queue, :missing_ids],
      %{count: count},
      %{backend_type: :consolidated_webhook, backend_id: backend_id}
    )
  end

  @spec drop(pos_integer(), [EncodedEvent.t() | LogEventPointer.t()], drop_reason()) :: :ok
  defp drop(backend_id, payloads, reason) do
    Enum.each(payloads, &delete_payload/1)
    emit_dropped(backend_id, length(payloads), reason)
  end

  @spec emit_dropped(pos_integer(), non_neg_integer(), drop_reason()) :: :ok
  defp emit_dropped(_backend_id, 0, _reason), do: :ok

  defp emit_dropped(backend_id, count, reason) do
    log_dropped(backend_id, count, reason)

    :telemetry.execute(
      [:logflare, :ingest_event_queue, :retry_dropped],
      %{count: count},
      %{backend_type: :consolidated_webhook, backend_id: backend_id, reason: reason}
    )
  end

  @doc """
  Logs a drop at most one time per backend per `#{@drop_log_interval_ms}` ms.

  A receiver that stays down fails every batch. Four batch processors then produce up
  to four warnings per second per backend per node. The telemetry still counts every
  drop. The last log time lives in the process dictionary of the batch processor.
  """
  @spec log_dropped(pos_integer(), pos_integer(), drop_reason()) :: :ok
  def log_dropped(backend_id, count, reason) do
    key = {__MODULE__, :last_drop_log, backend_id}
    now = System.monotonic_time(:millisecond)

    case Process.get(key) do
      last when is_integer(last) and now - last < @drop_log_interval_ms ->
        :ok

      _ ->
        Process.put(key, now)
        Logger.warning("Dropping #{count} webhook events: #{reason}", backend_id: backend_id)
        :ok
    end
  end

  @spec delete_payload(EncodedEvent.t() | LogEventPointer.t()) :: :ok
  defp delete_payload(payload) do
    pointer = pointer(payload)
    IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
    :ok
  end

  @spec pointer(EncodedEvent.t() | LogEventPointer.t()) :: LogEventPointer.t()
  defp pointer(%EncodedEvent{pointer: pointer}), do: pointer
  defp pointer(%LogEventPointer{} = pointer), do: pointer
end
