defmodule Logflare.Source.BigQuery.Pipeline do
  @moduledoc false
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.{Schema, BufferProducer}
  alias Logflare.Google.BigQuery.{GenUtils, EventUtils}
  alias Logflare.{Source}
  alias Logflare.{Users, Sources}
  alias Logflare.Source.Supervisor
  alias Logflare.{AccountEmail, Mailer}
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS

  def start_link(%RLS{source: source, plan: plan} = rls) do
    procs = calc_procs(source, plan)

    Broadway.start_link(__MODULE__,
      name: name(source.token),
      producer: [
        module: {BufferProducer, rls},
        hibernate_after: 30_000
      ],
      processors: [
        default: [concurrency: procs]
      ],
      batchers: [
        bq: [concurrency: procs, batch_size: 100, batch_timeout: 1000]
      ],
      context: rls
    )
  end

  @spec handle_message(any, Broadway.Message.t(), any) :: Broadway.Message.t()
  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  @spec handle_batch(:bq, list(Broadway.Message.t()), any, RLS.t()) :: [Broadway.Message.t()]
  def handle_batch(:bq, messages, _batch_info, %RLS{} = context) do
    stream_batch(context, messages)
  end

  def le_messages_to_bq_rows(messages) do
    Enum.map(messages, fn message ->
      le_to_bq_row(message.data)
    end)
  end

  def le_to_bq_row(%LE{body: body, id: id}) do
    {:ok, bq_timestamp} = DateTime.from_unix(body.timestamp, :microsecond)

    metadata =
      if map_size(body.metadata) > 0 do
        EventUtils.prepare_for_ingest(body.metadata)
      else
        body.metadata
      end

    body =
      Map.from_struct(body)
      |> Map.put(:timestamp, bq_timestamp)
      |> Map.put(:event_message, body.message)
      |> Map.put(:metadata, metadata)

    %Model.TableDataInsertAllRequestRows{
      insertId: id,
      json: body
    }
  end

  def stream_batch(%RLS{source_id: source_id} = context, messages) do
    Logger.metadata(source_id: source_id)

    rows = le_messages_to_bq_rows(messages)

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.

    case BigQuery.stream_batch!(context, rows) do
      {:ok,
       %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{
         insertErrors: nil
       }} ->
        messages

      {:ok,
       %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{
         insertErrors: errors
       }} ->
        Logger.warn("BigQuery insert errors.", error_string: inspect(errors))

        messages

      {:error, %Tesla.Env{} = response} ->
        case GenUtils.get_tesla_error_message(response) do
          "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
              message ->
            disconnect_backend_and_email(source_id, message)
            messages

          "The project" <> _tail = message ->
            # "The project web-wtc-1537199112807 has not enabled BigQuery."
            disconnect_backend_and_email(source_id, message)
            messages

          # Don't disconnect here because sometimes the GCP API doesn't find projects
          #
          # "Not found:" <> _tail = message ->
          #   disconnect_backend_and_email(source_id, message)
          #   messages

          _message ->
            Logger.warn("Stream batch response error!",
              tesla_response: GenUtils.get_tesla_error_message(response)
            )

            messages
        end

      {:error, :emfile = response} ->
        Logger.error("Stream batch emfile error!", tesla_response: response)
        messages

      {:error, :timeout = response} ->
        Logger.warn("Stream batch timeout error!", tesla_response: response)
        messages

      {:error, :checkout_timeout = response} ->
        Logger.warn("Stream batch checkout_timeout error!", tesla_response: response)
        messages

      {:error, response} ->
        Logger.warn("Stream batch unknown error!", tesla_response: inspect(response))
        messages
    end
  end

  defp process_data(%LE{source: %Source{lock_schema: true}} = log_event) do
    log_event
  end

  defp process_data(%LE{body: body, source: %Source{token: source_id}} = log_event) do
    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    if map_size(body.metadata) > 0 do
      Schema.update(source_id, log_event)
    end

    log_event
  end

  defp name(source_id) when is_atom(source_id) do
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

        AccountEmail.backend_disconnected(user, message)
        |> Mailer.deliver()

        Logger.warn("Backend disconnected for: #{user.email}",
          tesla_response: message
        )

      {:error, changeset} ->
        Logger.error("Failed to reset backend for user: #{user.email}",
          changeset: inspect(changeset)
        )
    end
  end

  defp calc_procs(source, plan) do
    limit =
      if plan.name == "Legacy",
        do: source.api_quota,
        else: plan.limit_rate_limit

    # Really only one worker per pipeline. Some contention probably in the buffer.
    Kernel.ceil(limit / 5000)
  end
end
