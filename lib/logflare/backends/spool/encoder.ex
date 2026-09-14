defmodule Logflare.Backends.Spool.Encoder do
  @moduledoc """
  Encodes one caller's chunk of `LogEvent`s into a single framed (raw,
  uncompressed) segment, ready to hand to a `Partition`. Runs in the
  ingest caller's own process. Compression happens separately, once for
  the whole accumulated file, at commit time (`Committer`).
  """

  alias Logflare.Backends.Spool.Framing
  alias Logflare.LogEvent

  @zstd_compression_level 3

  # The wire format version this build writes. Bump when the frame layout
  # or encoding scheme changes; never remove an old version's decode path.
  @current_version 2

  @spec current_version() :: pos_integer()
  def current_version, do: @current_version

  @doc """
  Extracts the version embedded in a file_key (e.g. `"0/uuid.v2.ndjson"` -> `2`),
  or `:legacy` if none is present.
  """
  @spec file_key_version(String.t()) :: pos_integer() | :legacy
  def file_key_version(file_key) do
    case Regex.run(~r/\.v(\d+)\./, file_key) do
      [_match, digits] -> String.to_integer(digits)
      nil -> :legacy
    end
  end

  @doc "Tags `key` with the current wire format version — see `current_version/0`."
  @spec file_key_with_version(String.t(), String.t()) :: String.t()
  def file_key_with_version(key, ext), do: "#{key}.v#{@current_version}.#{ext}"

  @doc "Builds a full, versioned spool file key for `index`."
  @spec build_file_key(non_neg_integer(), :ndjson | :etf, boolean(), :gzip | :zstd) :: String.t()
  def build_file_key(index, format, compress, algorithm) do
    ext = file_extension(format, compress, algorithm)
    file_key_with_version("#{index}/#{generate_uuidv7()}", ext)
  end

  @spec generate_uuidv7() :: String.t()
  def generate_uuidv7, do: UUIDv7.generate()

  @spec encode_chunk([LogEvent.t()], :ndjson | :etf) ::
          {segment :: binary(), raw_byte_size :: non_neg_integer()}
  def encode_chunk(log_events, format) do
    raw = encode_raw(log_events, format)
    segment = Framing.encode_segment(raw)
    {segment, byte_size(raw)}
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

  @doc "Compresses `data` with `algorithm`."
  @spec compress_binary(:gzip | :zstd, binary()) :: binary()
  def compress_binary(:gzip, data) do
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

  def compress_binary(:zstd, data), do: :ezstd.compress(data, @zstd_compression_level)
end
