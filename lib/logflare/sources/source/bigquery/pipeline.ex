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
  alias Logflare.Backends.BufferProducer
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.PubSubRates
  alias Logflare.Backends.Adaptor.BigQueryAdaptor

  require OpenTelemetry.Tracer

  # BQ max is 10MB
  # https://cloud.google.com/bigquery/quotas#streaming_inserts
  @max_batch_length 6_000_000
  @max_batch_size 500

  def start_link(args, opts \\ []) do
    {name, args} = Keyword.pop(args, :name)
    source = Keyword.get(args, :source)
    backend = Keyword.get(args, :backend)

    opts =
      Keyword.merge(
        [
          name: name,
          # top-level will apply to all children
          hibernate_after: 5_000,
          spawn_opt: [
            fullsweep_after: 10
          ],
          producer: [
            module:
              {BufferProducer,
               [
                 source_id: source.id,
                 backend_id: backend.id
               ]},
            transformer:
              {__MODULE__, :transform,
               [ref: {{source.id, backend.id, args[:pipeline_ref]}, source.token}]}
          ],
          processors: [
            default: [concurrency: 8, max_demand: 100]
          ],
          batchers: [
            bq: [
              concurrency: 16,
              batch_size: bq_batch_size_splitter(),
              batch_timeout: 1_500,
              # must be set when using custom batch_size splitter
              max_demand: @max_batch_size
            ]
          ],
          context: %{
            bigquery_project_id: args[:bigquery_project_id],
            bigquery_dataset_id: args[:bigquery_dataset_id],
            source_token: source.token,
            bq_storage_write_api: source.bq_storage_write_api,
            source_id: source.id,
            backend_id: Map.get(backend || %{}, :id)
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

  # Ziinc: temporarily pass in source token until PubSubRates is refactored
  def ack({queue, source_token}, successful, _failed) do
    # TODO: re-queue failed
    metrics = Sources.get_source_metrics_for_ingest(source_token)
    {sid, bid, _tid} = queue

    backend_metadata =
      if bid do
        Backends.Cache.get_backend(bid).metadata || %{}
      else
        %{}
      end

    source = Sources.Cache.get_by_id(sid)

    for %{data: le} <- successful do
      # delete immediately if not default backend or if avg rate is above 100
      if metrics.avg > 100 or bid != nil do
        IngestEventQueue.delete(queue, le)
      end

      # emit telemetry on event
      event_labels = Sources.get_labels_from_event(source, le)

      metrics = %{ingested_bytes: :erlang.external_size(le.body)}

      metadata =
        %{"source_id" => sid, "backend_id" => bid}
        |> Map.merge(event_labels)
        |> Map.merge(backend_metadata)

      :telemetry.execute([:logflare, :backends, :ingest], metrics, metadata)
    end
  end

  @spec handle_message(any, Broadway.Message.t(), any) :: Broadway.Message.t()
  def handle_message(_processor_name, message, context) do
    Logger.metadata(source_id: context.source_token, source_token: context.source_token)

    message
    |> Message.update_data(&process_data(&1, context))
    |> Message.put_batcher(:bq)
  end

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

    OpenTelemetry.Tracer.with_span :bigquery_pipeline, %{
      attributes: Map.new(attributes)
    } do
      source = Sources.Cache.get_by_id(context.source_id)

      if source && source.bq_storage_write_api do
        log_events = messages |> Enum.map(& &1.data)

        BigQueryAdaptor.insert_log_events_via_storage_write_api(log_events,
          project_id: context.bigquery_project_id,
          dataset_id: context.bigquery_dataset_id,
          source_id: context.source_id,
          source_token: context.source_token
        )

        messages
      else
        stream_batch(context, messages)
      end
    end
  end

  def le_messages_to_bq_rows(messages) do
    Enum.map(messages, fn message ->
      le_to_bq_row(message.data)
    end)
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

    %Model.TableDataInsertAllRequestRows{
      insertId: id,
      json: body
    }
  end

  def stream_batch(%{source_token: source_token} = context, messages) do
    Logger.metadata(source_id: source_token, source_token: source_token)

    :telemetry.span(
      [:logflare, :ingest, :pipeline, :stream_batch],
      %{source_token: source_token},
      fn -> execute_bigquery_stream_batch(context, messages) end
    )
  end

  defp execute_bigquery_stream_batch(%{source_token: source_token} = context, messages) do
    rows = le_messages_to_bq_rows(messages)

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.
    case BigQuery.stream_batch!(context, rows) do
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}} ->
        :ok

      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: errors}} ->
        Logger.warning("BigQuery insert errors.", error_string: inspect(errors))

      {:error, %Tesla.Env{} = response} ->
        case GenUtils.get_tesla_error_message(response) do
          "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
              message ->
            disconnect_backend_and_email(source_token, message)

          "The project" <> _tail = message ->
            # "The project web-wtc-1537199112807 has not enabled BigQuery."
            disconnect_backend_and_email(source_token, message)

          # Don't disconnect here because sometimes the GCP API doesn't find projects
          #
          # "Not found:" <> _tail = message ->
          #   disconnect_backend_and_email(source_id, message)
          #   messages

          _message ->
            Logger.warning("Stream batch response error!",
              tesla_response: GenUtils.get_tesla_error_message(response)
            )
        end

      {:error, response} ->
        Logger.warning("Stream batch unknown error!", tesla_response: inspect(response))
    end

    {messages, %{}}
  end

  def process_data(%LE{source_id: source_id} = log_event, context) do
    source = Sources.Cache.get_by_id(source_id)

    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that
    # didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    # random sample if local ingest rate is above a certain level
    # dynamic calculation maintains ~1 schema update per second across all rate levels
    unless source.lock_schema do
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

  defp disconnect_backend_and_email(source_id, message) when is_atom(source_id) do
    source = Sources.Cache.get_by(token: source_id)
    user = Users.Cache.get(source.user_id)

    defaults = %{
      bigquery_dataset_location: nil,
      bigquery_project_id: nil,
      bigquery_dataset_id: nil,
      bigquery_processed_bytes_limit: 10_000_000_000
    }

    case Users.update_user_allowed(user, defaults) do
      {:ok, user} ->
        Supervisor.reset_all_user_sources(user)

        user
        |> AccountEmail.backend_disconnected(message)
        |> Mailer.deliver()

        Logger.warning("Backend disconnected for: #{user.email}", tesla_response: message)

      {:error, changeset} ->
        Logger.error("Failed to reset backend for user: #{user.email}",
          changeset: inspect(changeset)
        )
    end
  end

  # https://hexdocs.pm/broadway/Broadway.html#start_link/2
  # split batch sizes based on json size
  # ensure that we are well below the 10MB limit
  def bq_batch_size_splitter do
    {
      {@max_batch_size, @max_batch_length},
      fn
        # reach max count, emit
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        # check content length
        message, {count, len} ->
          length = message_size(message.data.body)

          if len - length <= 0 do
            # below max batch count, but reach max batch length
            {:emit, {@max_batch_size, @max_batch_length}}
          else
            # below max batch count, below max batch length
            {:cont, {count - 1, len - length}}
          end
      end
    }
  end

  def message_size(data) do
    :erlang.external_size(data)
  end
end
