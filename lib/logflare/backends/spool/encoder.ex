defmodule Logflare.Backends.Spool.Encoder do
  @moduledoc """
  Encodes one caller's chunk of `LogEvent`s into a single framed (and
  optionally compressed) segment, ready to hand to a `Partition`.

  Compression now happens here, in the ingest caller's own process, at the
  scale of one request's chunk — not batched together with other callers'
  chunks the way `ProducerPipeline.handle_batch/4` used to. A chunk this
  size doesn't need the old incremental per-event zlib streaming (that
  existed specifically to avoid materializing a whole multi-thousand-event
  batch's raw bytes before compressing); building the full raw payload then
  compressing it in one shot is simpler and, per the benchmark that
  motivated this design, faster in practice at this scale.
  """

  alias Logflare.Backends.Spool.Framing
  alias Logflare.LogEvent

  @zstd_compression_level 3

  @spec encode_chunk([LogEvent.t()], :ndjson | :etf, boolean(), :gzip | :zstd) ::
          {segment :: binary(), compressed_byte_size :: non_neg_integer(),
           raw_byte_size :: non_neg_integer(), format_tag :: atom()}
  def encode_chunk(log_events, format, compress, algorithm) do
    raw = encode_raw(log_events, format)
    body = if compress, do: compress_binary(algorithm, raw), else: raw
    segment = Framing.encode_segment(body)
    {segment, byte_size(body), byte_size(raw), format_tag(format, compress, algorithm)}
  end

  @spec file_extension(:ndjson | :etf, boolean(), :gzip | :zstd) :: String.t()
  def file_extension(:ndjson, false, _algorithm), do: "ndjson"
  def file_extension(:etf, false, _algorithm), do: "etf"
  def file_extension(:ndjson, true, :gzip), do: "ndjson.gz"
  def file_extension(:ndjson, true, :zstd), do: "ndjson.zst"
  def file_extension(:etf, true, :gzip), do: "etf.gz"
  def file_extension(:etf, true, :zstd), do: "etf.zst"

  @spec content_type(:ndjson | :etf) :: String.t()
  def content_type(:ndjson), do: "application/x-ndjson"
  def content_type(:etf), do: "application/octet-stream"

  @spec content_encoding(boolean(), :gzip | :zstd) :: String.t() | nil
  def content_encoding(false, _algorithm), do: nil
  def content_encoding(true, :gzip), do: "gzip"
  def content_encoding(true, :zstd), do: "zstd"

  @spec format_tag(:ndjson | :etf, boolean(), :gzip | :zstd) :: atom()
  def format_tag(:ndjson, false, _algorithm), do: :ndjson
  def format_tag(:etf, false, _algorithm), do: :etf
  def format_tag(:ndjson, true, :gzip), do: :ndjson_gz
  def format_tag(:ndjson, true, :zstd), do: :ndjson_zstd
  def format_tag(:etf, true, :gzip), do: :etf_gz
  def format_tag(:etf, true, :zstd), do: :etf_zstd

  defp encode_raw(log_events, :ndjson) do
    log_events
    |> Enum.flat_map(fn log_event -> [encode_line(log_event), "\n"] end)
    |> IO.iodata_to_binary()
  end

  defp encode_raw(log_events, :etf) do
    log_events
    |> Enum.map(fn log_event ->
      %{
        id: log_event.id,
        source_id: log_event.source_id,
        body: log_event.body,
        event_type: log_event.event_type,
        ingested_at: DateTime.to_unix(log_event.ingested_at, :microsecond),
        via_rule_id: log_event.via_rule_id
      }
    end)
    |> :erlang.term_to_binary()
  end

  defp encode_line(log_event) do
    Jason.encode!(%{
      id: log_event.id,
      source_id: log_event.source_id,
      body: log_event.body,
      event_type: log_event.event_type,
      ingested_at: log_event.ingested_at,
      via_rule_id: log_event.via_rule_id
    })
  end

  defp compress_binary(:gzip, data) do
    z = :zlib.open()

    try do
      :ok = :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
      chunks = :zlib.deflate(z, data, :finish)
      :zlib.deflateEnd(z)
      IO.iodata_to_binary(chunks)
    after
      :zlib.close(z)
    end
  end

  defp compress_binary(:zstd, data), do: :ezstd.compress(data, @zstd_compression_level)
end
