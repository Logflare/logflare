defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWrite do
  @moduledoc """
  Encodes a Prometheus remote write v1 `WriteRequest` straight to protobuf wire format.

  Covers the subset of `prometheus/prompb` the adaptor sends, and produces the same
  bytes as the generic `Protobuf` encoder would for those messages:

      WriteRequest { repeated TimeSeries timeseries = 1; }
      TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
      Label        { string name = 1; string value = 2; }
      Sample       { double value = 1; int64 timestamp = 2; }

  Labels must already be sorted by name, as remote write requires.
  """

  import Bitwise

  @type label :: {name :: String.t(), value :: String.t()}
  @type sample :: {value :: float(), timestamp_ms :: integer()}
  @type series :: {[label()], [sample()]}

  @spec encode([series()]) :: binary()
  def encode(series) do
    series
    |> Enum.map(&encode_series/1)
    |> IO.iodata_to_binary()
  end

  defp encode_series({labels, samples}) do
    body = Enum.map(labels, &encode_label/1) ++ Enum.map(samples, &encode_sample/1)
    [0x0A, varint(IO.iodata_length(body)) | body]
  end

  defp encode_label({name, value}) do
    body = <<string_field(0x0A, name)::binary, string_field(0x12, value)::binary>>
    <<0x0A, varint(byte_size(body))::binary, body::binary>>
  end

  defp encode_sample({value, timestamp}) do
    body = <<double_field(value)::binary, int64_field(timestamp)::binary>>
    <<0x12, varint(byte_size(body))::binary, body::binary>>
  end

  # proto3 leaves out fields holding their default value.
  defp string_field(_tag, ""), do: <<>>
  defp string_field(tag, value), do: <<tag, varint(byte_size(value))::binary, value::binary>>

  # Only +0.0 is the default; -0.0 is written like any other value.
  defp double_field(value) when value === 0.0, do: <<>>
  defp double_field(value), do: <<0x09, value::float-little-64>>

  defp int64_field(0), do: <<>>
  defp int64_field(value), do: <<0x10, varint(value &&& 0xFFFF_FFFF_FFFF_FFFF)::binary>>

  # Recursing into a nested binary benchmarks faster here than appending to an
  # accumulator, whose 7-bit segments defeat the binary append optimization.
  defp varint(value) when value < 0x80, do: <<value>>
  defp varint(value), do: <<1::1, value::7, varint(value >>> 7)::binary>>
end
