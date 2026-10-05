defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWriteTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWrite

  describe "encode/1" do
    test "encodes an empty request as an empty binary" do
      assert RemoteWrite.encode([]) == ""
    end

    property "produces the same bytes as the generic protobuf encoder" do
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
            samples <- list_of(sample(), max_length: 4) do
      {Enum.sort(labels), samples}
    end
  end

  defp label do
    tuple({one_of([constant(""), string(:printable)]), one_of([constant(""), string(:utf8)])})
  end

  defp sample do
    tuple({
      one_of([constant(0.0), constant(-0.0), float()]),
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
            labels: for({name, value} <- labels, do: %Prometheus.Label{name: name, value: value}),
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
