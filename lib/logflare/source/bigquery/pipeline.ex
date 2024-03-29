defmodule Logflare.Source.BigQuery.Pipeline do
  @moduledoc false
  use Broadway

  require Logger

  alias Broadway.Message
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.AccountEmail
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.Mailer
  alias Logflare.Source
  alias Logflare.Source.BigQuery.BufferProducer
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.Supervisor
  alias Logflare.Sources
  alias Logflare.Users

  # each batch should at most be 5MB
  @max_batch_length 5_000_000
  @max_batch_size 250
  def start_link([%RLS{} = rls | opts]), do: start_link(rls, opts)
  def start_link(%RLS{} = rls), do: start_link(rls, [])

  def start_link(%RLS{source: source, plan: _plan} = rls, opts) do
    opts =
      Keyword.merge(
        [
          name: name(source.token),
          # top-level will apply to all children
          hibernate_after: 5_000,
          spawn_opt: [
            fullsweep_after: 0
          ],
          producer: [
            module: {BufferProducer, %{source_token: rls.source_id}}
          ],
          processors: [
            default: [concurrency: System.schedulers_online() * 2]
          ],
          batchers: [
            bq: [
              concurrency: System.schedulers_online() * 2,
              batch_size: bq_batch_size_splitter(),
              batch_timeout: 1_500,
              # must be set when using custom batch_size splitter
              max_demand: @max_batch_size
            ]
          ],
          context: %{
            bigquery_project_id: rls.bigquery_project_id,
            bigquery_dataset_id: rls.bigquery_dataset_id,
            source_token: rls.source_id
          }
        ],
        opts
      )

    Broadway.start_link(
      __MODULE__,
      opts
    )
  end

  @spec handle_message(any, Broadway.Message.t(), any) :: Broadway.Message.t()
  def handle_message(_processor_name, message, context) do
    Logger.metadata(source_id: context.source_token, source_token: context.source_token)

    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  def handle_batch(:bq, messages, batch_info, %{source_token: token} = context) do
    :telemetry.execute(
      [:logflare, :ingest, :pipeline, :handle_batch],
      %{batch_size: batch_info.size, batch_trigger: batch_info.trigger},
      %{
        source_token: token
      }
    )

    stream_batch(context, messages)
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

      {:error, :emfile = response} ->
        Logger.error("Stream batch emfile error!", tesla_response: response)

      {:error, :timeout = response} ->
        Logger.warning("Stream batch timeout error!", tesla_response: response)

      {:error, :checkout_timeout = response} ->
        Logger.warning("Stream batch checkout_timeout error!", tesla_response: response)

      {:error, response} ->
        Logger.warning("Stream batch unknown error!", tesla_response: inspect(response))
    end

    messages
  end

  def process_data(%LE{source: %Source{lock_schema: true}} = log_event) do
    log_event
  end

  def process_data(%LE{body: _body, source: %Source{token: source_id}} = log_event) do
    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.
    Schema.update(source_id, log_event)

    log_event
  end

  def name(source_id) when is_atom(source_id) do
    String.to_atom("#{source_id}" <> "-pipeline")
  end

  defp disconnect_backend_and_email(source_id, message) when is_atom(source_id) do
    source = Sources.Cache.get_by(token: source_id)
    user = Users.Cache.get_by(id: source.user_id)

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
  defp bq_batch_size_splitter() do
    {
      {@max_batch_size, @max_batch_length},
      fn
        # reach max count, emit
        _message, {1, _len} ->
          {:emit, {@max_batch_size, @max_batch_length}}

        # check content length
        message, {count, len} ->
          payload = Jason.encode!(message.data.body)
          length = IO.iodata_length(payload)

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
end
