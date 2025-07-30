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
  @spec ingest([map()], module(), Logflare.Source.t()) ::
          :ok | {:ok, count :: pos_integer()} | {:error, term()}
  def ingest(data, processor, %Logflare.Source{} = source)
      when is_list(data) and is_atom(processor) do
    metadata = %{
      processor: processor,
      source_token: source.token,
      source_id: source.id
    }

    :telemetry.span([:logflare, :logs, :processor, :ingest], metadata, fn ->
      batch =
        :telemetry.span([:logflare, :logs, :processor, :ingest, :handle_batch], metadata, fn ->
          {processor.handle_batch(data, source), metadata}
        end)

      :telemetry.execute(
        [:logflare, :logs, :processor, :ingest, :logs],
        %{
          count: length(batch)
        },
        metadata
      )

      :telemetry.span([:logflare, :logs, :processor, :ingest, :store], metadata, fn ->
        result = store(source, batch)

        new_meta = Map.merge(metadata, %{success: result == :ok})

        {{result, new_meta}, new_meta}
      end)
    end)
  end

  defp store(%Logflare.Source{v2_pipeline: true} = source, batch) do
    Backends.ensure_source_sup_started(source)
    Backends.ingest_logs(batch, source)
  end

  defp store(%Logflare.Source{v2_pipeline: false} = source, batch),
    do: Logs.ingest_logs(batch, source)
end
