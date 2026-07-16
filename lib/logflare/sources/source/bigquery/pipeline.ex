defmodule Logflare.Sources.Source.BigQuery.Pipeline do
  @moduledoc false
  use Broadway

  require Logger

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.AccountEmail
  alias Logflare.Backends
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Mailer
  alias Logflare.Sources
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.Backends.BufferProducer
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.PubSubRates
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Utils
  require OpenTelemetry.Tracer

  @behaviour Broadway.Acknowledger

  # BQ max is 10MB
  # https://cloud.google.com/bigquery/quotas#streaming_inserts
  @max_batch_length 6_000_000
  @max_batch_size 500
  @max_retries 0

  def start_link(args, opts \\ []) do
    {name, args} = Keyword.pop(args, :name)
    source = Keyword.get(args, :source)
    backend = Keyword.get(args, :backend)

    max_retries =
      Application.get_env(:logflare, :bigquery_pipeline, [])
      |> Keyword.get(:max_retries, @max_retries)

    ack_config = %{max_retries: max_retries}

    opts =
      Keyword.merge(
        [
          # top-level will apply to all children
          name: name,
          hibernate_after: 5_000,
          spawn_opt: [
            fullsweep_after: 10
          ],
          producer: [
            module:
              {BufferProducer,
               [
                 source_id: source.id,
                 backend_id: backend.id,
                 id_passing: true
               ]},
            transformer:
              {__MODULE__, :transform,
               [
                 ref: {{source.id, backend.id, args[:pipeline_ref]}, ack_config}
               ]}
          ],
          processors: [
            default: [concurrency: 8, max_demand: 100]
          ],
          batchers: [
            bq: [
              concurrency: 16,
              batch_size: bq_batch_size_splitter(),
              batch_timeout: 1_500,
              # required when using a custom batch_size splitter
              max_demand: @max_batch_size
            ]
          ],
          context: %{
            bigquery_project_id: args[:bigquery_project_id],
            bigquery_dataset_id: args[:bigquery_dataset_id],
            source_token: source.token,
            bq_storage_write_api: source.bq_storage_write_api,
            source_id: source.id,
            backend_id: Map.get(backend || %{}, :id),
            user_id: source.user_id,
            system_source: source.system_source
          }
        ],
        opts
      )

    Broadway.start_link(
      __MODULE__,
      opts
    )
  end

  # pipeline name is sharded
  @impl Broadway
  def process_name({:via, module, {registry, identifier}}, base_name) do
    {:via, module, {registry, {identifier, base_name}}}
  end

  def process_name(proc_name, base_name) do
    String.to_atom("#{proc_name}-#{base_name}")
  end

  # Broadway transformer for custom producer
  def transform(event, args) do
    ref = args[:ref]

    %Message{
      data: event,
      acknowledger: {__MODULE__, ref, :ack_data}
    }
  end

  @impl Broadway.Acknowledger
  def ack({queue, config}, successful, failed) do
    {sid, bid, _pipeline_ref} = queue

    finalize_acked_events({sid, bid}, successful)
    maybe_requeue_failed({sid, bid}, failed, config)

    :telemetry.execute(
      [:logflare, :backends, :pipeline, :ack],
      %{successful: length(successful), failed: length(failed)},
      %{}
    )

    :ok
  end

  # Always deletes the generation-store row. If the source's ingest rate is low, also
  # keeps an independent copy in the recent-events cache first, so "recent logs" reads
  # (list_recent_logs_local/2, Sources.source_idle?/1) still see it long after the row
  # itself — and its generation — is gone.
  @spec finalize_acked_events(IngestEventQueue.queues_key(), [Message.t()]) :: :ok
  defp finalize_acked_events(_queues_key, []), do: :ok

  defp finalize_acked_events({sid, bid} = queues_key, successful) do
    record? = bid == nil and should_record_recent?(sid)

    Enum.each(successful, fn %{data: %LogEventPointer{} = pointer} ->
      if record?, do: record_recent_copy(queues_key, pointer)
      IngestEventQueue.delete_id(pointer.tid, pointer.id)
    end)
  end

  defp record_recent_copy(queues_key, %LogEventPointer{} = pointer) do
    case IngestEventQueue.lookup_event(pointer.tid, pointer.id) do
      nil -> :ok
      event -> IngestEventQueue.record_recent_event(queues_key, event)
    end
  end

  # No source to resolve (e.g. it was deleted mid-flight) means no ingest rate to check
  # and no source-scoped "recent logs" reader to serve — skip the cache instead of
  # guessing at a default rate.
  @spec should_record_recent?(pos_integer()) :: boolean()
  defp should_record_recent?(sid) do
    case Sources.Cache.get_by_id(sid) do
      nil -> false
      source -> Sources.get_source_metrics_for_ingest(source).avg <= 100
    end
  end

  @impl Broadway
  def handle_message(_processor_name, message, context) do
    Logger.metadata(
      source_id: context.source_token,
      source_token: context.source_token,
      user_id: context.user_id,
      system_source: context.system_source
    )

    Message.put_batcher(message, :bq)
  end

  @spec bq_batch_size_splitter() ::
          {{non_neg_integer(), non_neg_integer()},
           (Message.t(), {non_neg_integer(), non_neg_integer()} ->
              {:emit | :cont, {non_neg_integer(), non_neg_integer()}})}
  defp bq_batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        message, {count, len} ->
          size = message_size(message)

          if len - size <= 0 do
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            {:cont, {count - 1, len - size}}
          end
      end
    }
  end

  defp message_size(%{data: %LogEventPointer{size: size}}), do: size

  @impl Broadway
  def handle_batch(:bq, messages, batch_info, context) do
    :telemetry.execute(
      [:logflare, :backends, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        backend_type: :bigquery
      }
    )

    attributes =
      for {k, v} <- [
            source_id: context.source_id,
            source_token: context.source_token,
            backend_id: context.backend_id,
            ingest_batch_size: batch_info.size,
            ingest_batch_trigger: batch_info.trigger
          ],
          v != nil,
          do: {k, v}

    OpenTelemetry.Tracer.with_span "ingest.bigquery_batch", %{
      attributes: Map.new(attributes)
    } do
      source = Sources.Cache.get_by_id(context.source_id)

      # Fetch full LogEvents from ETS. Sizes were computed in the producer and are
      # carried on each message — no recomputation needed here. The batch is already
      # byte-bounded by bq_batch_size_splitter/0 in the batcher config.
      {triples, missing} = fetch_events_from_messages(messages, context, source)

      if missing != [] do
        :telemetry.execute(
          [:logflare, :ingest_event_queue, :missing_ids],
          %{count: length(missing)},
          %{source_id: context.source_id}
        )
      end

      {log_events, batch_count, batch_size} = collect_batch_events(triples)

      if source && source.bq_storage_write_api do
        batch_attrs = compute_batch_attrs(batch_count, batch_size, :bq_storage_write)

        OpenTelemetry.Tracer.with_span "ingest.bq_insert", %{attributes: batch_attrs} do
          BigQueryAdaptor.insert_log_events_via_storage_write_api(log_events,
            project_id: context.bigquery_project_id,
            dataset_id: context.bigquery_dataset_id,
            source_id: context.source_id,
            source_token: context.source_token,
            backend_id: context.backend_id
          )
        end
      else
        batch_attrs = compute_batch_attrs(batch_count, batch_size, :bq_streaming_insert)

        OpenTelemetry.Tracer.with_span "ingest.bq_insert", %{attributes: batch_attrs} do
          stream_batch(context, log_events)
        end
      end

      emit_ingest_telemetry(context, source, triples)

      succeeded = Enum.map(triples, fn {msg, _le, _size} -> msg end)
      failed_missing = Enum.map(missing, &Message.failed(&1, "missing from ETS"))

      case failed_missing do
        [] -> succeeded
        _ -> succeeded ++ failed_missing
      end
    end
  end

  @spec le_list_to_bq_rows([LE.t()]) :: [Model.TableDataInsertAllRequestRows.t()]
  def le_list_to_bq_rows(log_events) do
    Enum.map(log_events, &le_to_bq_row/1)
  end

  defp fetch_events_from_messages(messages, context, source) do
    Enum.reduce(messages, {[], []}, fn message, {out, missing} ->
      case {lookup_message_event(message), message_size(message)} do
        {nil, _size} ->
          {out, [message | missing]}

        {log_event, size} ->
          {[{message, process_data(log_event, context, source), size} | out], missing}
      end
    end)
  end

  defp lookup_message_event(%{data: %LogEventPointer{tid: tid, id: id}}) do
    IngestEventQueue.lookup_event(tid, id)
  end

  # Single pass collecting log events + batch metrics. Separate accumulator args (rather
  # than a tuple-accumulator Enum.reduce) avoid allocating a fresh tuple per event. The
  # cons-built list is reversed relative to `triples`; output order is insignificant since
  # each BQ row carries its own insertId.
  @spec collect_batch_events([{Message.t(), LE.t(), non_neg_integer()}]) ::
          {[LE.t()], non_neg_integer(), non_neg_integer()}
  defp collect_batch_events(triples), do: collect_batch_events(triples, [], 0, 0)

  defp collect_batch_events([], log_events, count, bytes), do: {log_events, count, bytes}

  defp collect_batch_events([{_msg, le, size} | rest], log_events, count, bytes) do
    collect_batch_events(rest, [le | log_events], count + 1, bytes + size)
  end

  def le_to_bq_row(%LE{body: body, id: id}) do
    {:ok, bq_timestamp} = DateTime.from_unix(body["timestamp"], :microsecond)

    body =
      for {k, v} <- body, into: %{} do
        if is_map(v) do
          {k, EventUtils.prepare_for_ingest(v)}
        else
          {k, v}
        end
      end
      |> Map.put("timestamp", bq_timestamp)
      |> Map.put("event_message", body["event_message"])
      |> case do
        %{"start_time" => start_time, "end_time" => end_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{
            data
            | "start_time" => DateTime.from_unix!(start_time, :nanosecond),
              "end_time" => DateTime.from_unix!(end_time, :nanosecond)
          }

        %{"start_time" => start_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{data | "start_time" => DateTime.from_unix!(start_time, :nanosecond)}

        %{"end_time" => end_time} = data
        when is_map_key(data, "resource") and is_map_key(data, "scope") ->
          # round to microseconds
          %{data | "end_time" => DateTime.from_unix!(end_time, :nanosecond)}

        data ->
          data
      end

    %Model.TableDataInsertAllRequestRows{
      insertId: id,
      json: body
    }
  end

  def stream_batch(
        %{source_token: source_token, user_id: user_id, system_source: system_source} = context,
        log_events
      ) do
    Logger.metadata(
      source_id: source_token,
      source_token: source_token,
      user_id: user_id,
      system_source: system_source
    )

    :telemetry.span(
      [:logflare, :ingest, :pipeline, :stream_batch],
      %{source_token: source_token},
      fn -> execute_bigquery_stream_batch(context, log_events) end
    )
  end

  defp execute_bigquery_stream_batch(%{source_token: source_token} = context, log_events) do
    rows =
      OpenTelemetry.Tracer.with_span "ingest.bq_serialize", %{
        attributes: %{insert_method: :bq_streaming_insert}
      } do
        result = le_list_to_bq_rows(log_events)
        OpenTelemetry.Tracer.set_attribute(:serialized_bytes, :erlang.external_size(result))
        result
      end

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.
    OpenTelemetry.Tracer.with_span "ingest.bq_api_call", %{
      attributes: %{insert_method: :bq_streaming_insert}
    } do
      case BigQuery.stream_batch!(context, rows) do
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, 0)
          :ok

        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: errors}} ->
          OpenTelemetry.Tracer.set_attribute(:insert_error_count, length(errors))
          error_string = inspect(errors)
          OpenTelemetry.Tracer.set_status(:error, error_string)
          Logger.warning("BigQuery insert errors.", error_string: error_string)

        {:error, %Tesla.Env{} = response} ->
          message = GenUtils.get_tesla_error_message(response)
          OpenTelemetry.Tracer.set_status(:error, message)

          case message do
            "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
                message ->
              disconnect_backend_and_email(source_token, message)

            "The project" <> _tail = message ->
              # "The project web-wtc-1537199112807 has not enabled BigQuery."
              disconnect_backend_and_email(source_token, message)

            _message ->
              Logger.warning("Stream batch response error!",
                tesla_response: GenUtils.get_tesla_error_message(response)
              )
          end

        {:error, response} ->
          OpenTelemetry.Tracer.set_status(:error, inspect(response))

          Logger.warning("Stream batch unknown error!",
            tesla_response: inspect(response, limit: 20)
          )
      end
    end

    {log_events, %{}}
  end

  @spec process_data(LE.t(), map(), Sources.Source.t() | nil) :: LE.t()
  def process_data(%LE{} = log_event, context, source) do
    # Source is resolved once per batch in handle_batch/4 and reused, not re-fetched per event.

    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that
    # didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    # random sample if local ingest rate is above a certain level
    # dynamic calculation maintains ~1 schema update per second across all rate levels
    if source && not source.lock_schema do
      probability =
        case PubSubRates.Cache.get_local_rates(source.token) do
          %{average_rate: avg} when avg > 0 ->
            # probability = 1.0 / avg with safety bounds
            # supports rates up to 100K+/sec: at 100K/sec -> 0.00001 (samples ~1/sec)
            min(1.0, max(0.00001, 1.0 / avg))

          _ ->
            1.0
        end

      if :rand.uniform() <= probability do
        :ok =
          Backends.via_source(source, {Schema, Map.get(context, :backend_id)})
          |> Schema.update(log_event, source)
      end
    end

    log_event
  end

  def name(source_id) when is_atom(source_id) do
    String.to_atom("#{source_id}" <> "-pipeline")
  end

  @spec compute_batch_attrs(non_neg_integer(), non_neg_integer(), atom()) :: map()
  defp compute_batch_attrs(batch_count, batch_size, bq_api_tag) do
    %{
      insert_method: bq_api_tag,
      batch_event_count: batch_count,
      batch_bytes: batch_size
    }
  end

  defp disconnect_backend_and_email(source_id, message) when is_atom(source_id) do
    source = Sources.Cache.get_by(token: source_id)
    user = Users.Cache.get(source.user_id)

    defaults = %{
      bigquery_dataset_location: nil,
      bigquery_project_id: nil,
      bigquery_dataset_id: nil,
      bigquery_processed_bytes_limit: 10_000_000_000
    }

    Logger.warning("user audit: BigQuery backend auto-disconnect triggered",
      action: "user.bq_auto_disconnect",
      user_id: user.id,
      user_email: user.email,
      source_token: source_id,
      reason: message
    )

    case Users.update_user_allowed(user, defaults) do
      {:ok, user} ->
        Supervisor.reset_all_user_sources(user)

        user
        |> AccountEmail.backend_disconnected(message)
        |> Mailer.deliver()

        Logger.warning("user audit: BigQuery backend auto-disconnected",
          action: "user.bq_auto_disconnected",
          user_id: user.id,
          user_email: user.email,
          source_token: source_id,
          reason: message
        )

      {:error, changeset} ->
        Logger.error("user audit: BigQuery backend auto-disconnect failed",
          action: "user.bq_auto_disconnect_failed",
          user_id: user.id,
          user_email: user.email,
          source_token: source_id,
          reason: message,
          errors: inspect(changeset.errors)
        )
    end
  end

  # Requeue failed events if the number of previous retries is less than max_retries.
  defp maybe_requeue_failed(_sid_bid, [], _config), do: :ok

  defp maybe_requeue_failed(_sid_bid, failed, %{max_retries: 0}) do
    pointers = Enum.map(failed, fn %{data: %LogEventPointer{} = pointer} -> pointer end)
    drop_pointers(pointers, "retries disabled")
  end

  defp maybe_requeue_failed(sid_bid, failed, %{max_retries: max_retries}) do
    {retriable, exhausted} =
      failed
      |> Enum.map(fn %{data: %LogEventPointer{} = pointer} -> pointer end)
      |> Enum.split_with(&(&1.retries < max_retries))

    drop_pointers(exhausted, "exhausted #{max_retries} retries")
    requeue_retriable(sid_bid, retriable)
  end

  defp requeue_retriable(_sid_bid, []), do: :ok

  defp requeue_retriable(sid_bid, retriable) do
    Logger.info("Requeuing #{length(retriable)} BigQuery events for retry")

    events =
      for pointer <- retriable,
          event = IngestEventQueue.lookup_event(pointer.tid, pointer.id),
          not is_nil(event) do
        IngestEventQueue.delete_id(pointer.tid, pointer.id)
        %{event | retries: pointer.retries + 1}
      end

    emit_requeue_lookup_miss_telemetry(sid_bid, length(retriable) - length(events))

    if events != [], do: IngestEventQueue.add_to_table(sid_bid, events)

    :ok
  end

  # A miss here means the pointer's generation was already dropped by
  # GenerationJanitor before the retry could look it up — the event is silently
  # lost rather than requeued. Bounded and rare in practice, but worth surfacing
  # since it's otherwise invisible.
  @spec emit_requeue_lookup_miss_telemetry({pos_integer(), pos_integer()}, non_neg_integer()) ::
          :ok
  defp emit_requeue_lookup_miss_telemetry(_sid_bid, 0), do: :ok

  defp emit_requeue_lookup_miss_telemetry({sid, bid}, missing_count) do
    Logger.warning(
      "Dropped #{missing_count} BigQuery event(s) during retry requeue: pointer's generation was already gone by lookup time",
      source_id: sid,
      backend_id: bid
    )

    :telemetry.execute(
      [:logflare, :ingest_event_queue, :requeue_lookup_miss],
      %{count: missing_count},
      %{source_id: sid, backend_id: bid}
    )
  end

  defp drop_pointers([], _reason), do: :ok

  defp drop_pointers(pointers, reason) do
    Logger.warning("Dropping #{length(pointers)} BigQuery events: #{reason}")

    Enum.each(pointers, fn pointer -> IngestEventQueue.delete_id(pointer.tid, pointer.id) end)
  end

  # Emit per-event ingest telemetry from handle_batch, where the full LogEvent is
  # already in hand (no extra ETS lookup in ack). The label mapping and backend
  # metadata are resolved once per batch rather than per event.
  defp emit_ingest_telemetry(_context, nil, _triples), do: :ok

  defp emit_ingest_telemetry(context, source, triples) do
    label_mapping = Sources.get_labels_mapping(source)
    backend_metadata = backend_metadata(context.backend_id)
    queue = {context.source_id, context.backend_id, nil}

    for {_msg, le, size} <- triples do
      event_labels = Sources.extract_labels(label_mapping, le)
      emit_event_telemetry(queue, source, event_labels, size, backend_metadata)
    end
  end

  defp backend_metadata(nil), do: %{}

  defp backend_metadata(bid) do
    case Backends.Cache.get_backend(bid) do
      %{metadata: metadata} -> metadata || %{}
      _ -> %{}
    end
  end

  defp emit_event_telemetry({sid, bid, _}, source, event_labels, size, backend_metadata) do
    metrics = %{ingested_bytes: size}

    metadata =
      %{
        "source_id" => sid,
        "backend_id" => bid,
        "source_uuid" => Utils.stringify(source.token),
        "user_id" => source.user_id,
        "system_source" => source.system_source
      }
      |> Map.merge(event_labels)
      |> Map.merge(backend_metadata)

    :telemetry.execute([:logflare, :backends, :ingest], metrics, metadata)
  end
end
