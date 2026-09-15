defmodule Logflare.Backends.Spool.Encoder do
  @moduledoc """
  Encodes one caller's chunk of `LogEvent`s into a single raw (unframed)
  binary, ready to hand to `DurableBuffer.append/3` — the buffer frames it
  itself (`DurableBuffer.WAL.encode/1`) as part of its own group commit.
  Runs in the ingest caller's own process. Compression happens separately,
  once for the whole accumulated file, at commit time
  (`DurableBuffer.Backends.Cloud`).
  """

  alias Logflare.LogEvent

  @zstd_compression_level 3

  # The wire format version this build writes. Bump when the frame layout
  # or encoding scheme changes; never remove an old version's decode path.
  @current_version 2

  @spec current_version() :: pos_integer()
  def current_version, do: @current_version

  @doc """
  Extracts the version embedded in a file_key (e.g. `"0/uuid.v2.etf"` -> `2`),
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
  @spec build_file_key(non_neg_integer(), boolean()) :: String.t()
  def build_file_key(index, compress) do
    file_key_with_version("#{index}/#{generate_uuidv7()}", file_extension(compress))
  end

  @spec generate_uuidv7() :: String.t()
  def generate_uuidv7, do: UUIDv7.generate()

  @spec encode_raw_chunk([LogEvent.t()]) :: binary()
  def encode_raw_chunk(log_events), do: encode_raw(log_events)

  @spec file_extension(boolean()) :: String.t()
  def file_extension(false), do: "etf"
  def file_extension(true), do: "etf.zst"

  @spec content_type() :: String.t()
  def content_type, do: "application/octet-stream"

  @spec content_encoding(boolean()) :: String.t() | nil
  def content_encoding(false), do: nil
  def content_encoding(true), do: "zstd"

  @doc "Storage upload headers (content-type, and content-encoding when `compress` is true)."
  @spec upload_headers(boolean()) :: [headers: %{String.t() => String.t()}]
  def upload_headers(compress) do
    base = %{"content-type" => content_type()}

    headers =
      case content_encoding(compress) do
        nil -> base
        encoding -> Map.put(base, "content-encoding", encoding)
      end

    [headers: headers]
  end

  defp encode_raw(log_events) do
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

  @doc "Compresses `data` with zstd."
  @spec compress_binary(binary()) :: binary()
  def compress_binary(data), do: :ezstd.compress(data, @zstd_compression_level)
end
