defmodule Logflare.Logs.Processor do
  @moduledoc """
  Processor definition for logs ingestion.

  This module define behaviour for processing logs from external format (one
  coming from the client) to the internal one that is used by the application
  itself.
  """

  alias Logflare.Backends
  alias Logflare.Sources.Source

  @doc """
  Translate `data` into format that will be used for storage.
  """
  @callback handle_batch(data :: [map()], source :: Logflare.Sources.Source.t()) :: [map()]

  @doc """
  Process `data` using `processor` to translate from incoming format to storage format.

  The function first makes sure that the `SourceSup` of the source is up. That wait has a time
  limit (see `Logflare.Backends.start_source_sup/1`). When the `SourceSup` is still not up after
  the wait, the function does not ingest and returns `{:error, :source_unavailable}`. The write
  is not certain then, so the caller must tell the client to send the batch again.

  When the source was deleted after the caller loaded it, the function does not ingest and
  returns `{:error, :source_not_found}`.
  """
  @spec ingest([map()], module(), Logflare.Sources.Source.t()) ::
          :ok
          | {:ok, count :: pos_integer()}
          | {:error, :source_unavailable | :source_not_found | term()}
  def ingest(data, processor, %Source{} = source)
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
        # allow_spooling: true — this is the genuine client-submitted entry
        # point (every log_controller.ex/gRPC ingestion action funnels
        # through here), as opposed to SourceRouter's re-entrant calls,
        # which never pass this and so can never be spooled.
        result = store(batch, source)

        new_meta = Map.merge(metadata, %{success: elem(result, 0) == :ok})

        {{result, new_meta}, new_meta}
      end)
    end)
  end

  @spec store([map()], Source.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  defp store(batch, source) do
    case Backends.ensure_source_sup_started(source) do
      :ok -> Backends.ingest_logs(batch, source, nil, true)
      {:error, :not_found} -> {:error, :source_not_found}
      {:error, _reason} -> {:error, :source_unavailable}
    end
  end
end
