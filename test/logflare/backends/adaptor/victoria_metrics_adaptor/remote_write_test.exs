defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWriteTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWrite

  describe "encode/1" do
    test "encodes an empty request as an empty binary" do
      assert RemoteWrite.encode([]) == ""
    end

    test "sorts labels by name before encoding" do
      labels = [{"z", "last"}, {"__name__", "metric"}, {"a", "first"}]
      samples = [{1.0, 7}]

      for labels <- [labels, Map.new(labels)] do
        series = [{labels, samples}]
        encoded = RemoteWrite.encode(series)
        assert encoded == series |> to_write_request() |> Protobuf.encode()

        assert [series] = Prometheus.WriteRequest.decode(encoded).timeseries
        assert Enum.map(series.labels, & &1.name) == ["__name__", "a", "z"]
        assert [%{value: 1.0, timestamp: 7}] = series.samples
      end
    end

    test "encodes label lengths across protobuf varint boundaries" do
      for size <- [0, 127, 128, 16_383, 16_384] do
        name = String.duplicate("n", size)
        value = String.duplicate("λ", div(size, 2)) <> String.duplicate("x", rem(size, 2))
        series = [{[{name, value}], []}]

        assert RemoteWrite.encode(series) == series |> to_write_request() |> Protobuf.encode()
      end
    end

    property "produces the same bytes as the generic protobuf encoder with sorted labels" do
      check all series <- list_of(series(), max_length: 5), max_runs: 300 do
        assert RemoteWrite.encode(series) == series |> to_write_request() |> Protobuf.encode()
      end
    end

    property "round-trips through the protobuf decoder" do
      check all series <- list_of(series(), max_length: 5), max_runs: 300 do
        decoded = series |> RemoteWrite.encode() |> Prometheus.WriteRequest.decode()
        assert decoded == to_write_request(series)
      end
    end
  end

  defp series do
    gen all labels <- list_of(label(), max_length: 6),
            labels_as_map? <- boolean(),
            samples <- list_of(sample(), max_length: 4) do
      {if(labels_as_map?, do: Map.new(labels), else: labels), samples}
    end
  end

  defp label do
    tuple({one_of([constant(""), string(:printable)]), one_of([constant(""), string(:utf8)])})
  end

  defp sample do
    tuple({
      one_of([
        constant(0.0),
        constant(-0.0),
        member_of([:nan, :infinity, :negative_infinity]),
        float()
      ]),
      one_of([
        constant(0),
        integer(),
        integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807)
      ])
    })
  end

  defp to_write_request(series) do
    %Prometheus.WriteRequest{
      timeseries:
        for {labels, samples} <- series do
          %Prometheus.TimeSeries{
            labels:
              for(
                {name, value} <- Enum.sort(labels),
                do: %Prometheus.Label{name: name, value: value}
              ),
            samples:
              for(
                {value, timestamp} <- samples,
                do: %Prometheus.Sample{value: value, timestamp: timestamp}
              )
          }
        end
    }
  end
end
