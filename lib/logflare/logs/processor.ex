defmodule Logflare.Logs.Processor do
  @moduledoc """
  Processor definition for logs ingestion.

  This module define behaviour for processing logs from external format (one
  coming from the client) to the internal one that is used by the application
  itself.
  """

  require Logger

  alias Logflare.Backends
  alias Logflare.Sources.Source

  @doc """
  Translate `data` into format that will be used for storage.
  """
  @callback handle_batch(data :: [map()], source :: Logflare.Sources.Source.t()) :: [map()]

  @doc """
  Process `data` using `processor` to translate from incoming format to storage format.
  """
  @spec ingest([map()], module(), Logflare.Sources.Source.t()) ::
          :ok | {:ok, count :: pos_integer()} | {:error, term()}
  def ingest(data, processor, %Source{} = source)
      when is_list(data) and is_atom(processor) do
    metadata = %{
      processor: processor,
      source_token: source.token,
      source_id: source.id
    }

    :telemetry.span([:logflare, :logs, :processor, :ingest], metadata, fn ->
      Logger.info(
        "[Processor] Starting ingestion for source_id=#{source.id} token=#{source.token} with #{length(data)} raw events"
      )

      batch =
        :telemetry.span([:logflare, :logs, :processor, :ingest, :handle_batch], metadata, fn ->
          {processor.handle_batch(data, source), metadata}
        end)

      Logger.info("[Processor] Processed #{length(batch)} events for source_id=#{source.id}")

      :telemetry.execute(
        [:logflare, :logs, :processor, :ingest, :logs],
        %{
          count: length(batch)
        },
        metadata
      )

      :telemetry.span([:logflare, :logs, :processor, :ingest, :store], metadata, fn ->
        Backends.ensure_source_sup_started(source)

        Logger.info(
          "[Processor] Calling Backends.ingest_logs with #{length(batch)} events for source_id=#{source.id}"
        )

        result = Backends.ingest_logs(batch, source)

        Logger.info(
          "[Processor] Backends.ingest_logs returned #{inspect(result)} for source_id=#{source.id}"
        )

        new_meta = Map.merge(metadata, %{success: elem(result, 0) == :ok})

        {{result, new_meta}, new_meta}
      end)
    end)
  end
end
