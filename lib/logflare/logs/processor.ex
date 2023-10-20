defmodule Logflare.Logs.Processor do
  @moduledoc """
  Processor definition for logs ingestion.

  This module define behaviour for processing logs from external format (one
  coming from the client) to the internal one that is used by the application
  itself.
  """

  alias Logflare.Backends
  alias Logflare.Logs

  @doc """
  Translate `data` into format that will be used for storage.
  """
  @callback handle_batch(data :: [map()], source :: Logflare.Source.t()) :: [map()]

  @doc """
  Process `data` using `processor` to translate from incoming format to storage format.
  """
  @spec ingest([map()], module(), Logflare.Source.t()) :: :ok | {:error, term()}
  def ingest(data, processor, %Logflare.Source{} = source)
      when is_list(data) and is_atom(processor) do
    metadata = %{
      processor: processor,
      source: source
    }

    :telemetry.span([:logflare, :logs, :processor, :ingest], metadata, fn ->
      batch = processor.handle_batch(data, source)

      result =
        if source.v2_pipeline do
          Backends.start_source_sup(source)
          Backends.ingest_logs(batch, source)
        else
          Logs.ingest_logs(batch, source)
        end

      {result, Map.merge(metadata, %{success: result == :ok})}
    end)
  end
end
