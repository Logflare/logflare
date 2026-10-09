defmodule Logflare.SystemMetrics.ClusterTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.SystemMetrics.Cluster
  alias Logflare.TestUtils

  describe "finch/0" do
    setup do
      TestUtils.attach_forwarder([:logflare, :system, :finch])
      :ok
    end

    test "aggregates S3 origins and pool instances without endpoint-derived labels" do
      first_origin = {:https, "private-bucket.s3.amazonaws.com", 443}
      second_origin = {:http, "private-minio.internal", 9000}
      missing_origin = {:https, "removed.example.com", 443}
      parent = self()

      for origin <- [first_origin, second_origin, missing_origin] do
        {:ok, _} = Registry.register(Logflare.FinchS3, origin, Finch.HTTP1.Pool)
      end

      start_supervised!(
        {Task,
         fn ->
           {:ok, _} = Registry.register(Logflare.FinchS3, first_origin, Finch.HTTP1.Pool)
           send(parent, :duplicate_registered)

           receive do
             :stop -> :ok
           end
         end}
      )

      assert_receive :duplicate_registered

      stub(Finch, :get_pool_status, fn
        Logflare.FinchS3, ^first_origin ->
          send(parent, {:queried, first_origin})

          {:ok,
           [
             %Finch.HTTP1.PoolMetrics{in_use_connections: 2, available_connections: 48},
             %Finch.HTTP1.PoolMetrics{in_use_connections: 3, available_connections: 47}
           ]}

        Logflare.FinchS3, ^second_origin ->
          {:ok, [%Finch.HTTP1.PoolMetrics{in_use_connections: 1, available_connections: 49}]}

        _pool, _origin ->
          {:error, :not_found}
      end)

      Cluster.finch()

      assert_received {:telemetry_event, [:logflare, :system, :finch],
                       %{
                         in_flight_requests: 6,
                         in_use_connections: 6,
                         available_connections: 144
                       }, %{pool: "Elixir.Logflare.FinchS3", url: "all"}}

      assert_received {:queried, ^first_origin}
      refute_received {:queried, ^first_origin}
    end

    test "emits zero S3 gauges when no registered origin has pool metrics" do
      stub(Finch, :get_pool_status, fn _pool, _origin -> {:error, :not_found} end)

      Cluster.finch()

      assert_received {:telemetry_event, [:logflare, :system, :finch],
                       %{
                         in_flight_requests: 0,
                         in_use_connections: 0,
                         available_connections: 0
                       }, %{pool: "Elixir.Logflare.FinchS3", url: "all"}}
    end

    test "preserves the existing per-origin pool metrics" do
      stub(Finch, :get_pool_status, fn
        Logflare.FinchDefault, "https://bigquery.googleapis.com" ->
          {:ok, [%{in_flight_requests: 7, in_use_connections: 2, available_connections: 48}]}

        _pool, _origin ->
          {:error, :not_found}
      end)

      Cluster.finch()

      assert_received {:telemetry_event, [:logflare, :system, :finch],
                       %{
                         in_flight_requests: 7,
                         in_use_connections: 2,
                         available_connections: 48
                       },
                       %{
                         pool: "Elixir.Logflare.FinchDefault",
                         url: "https://bigquery.googleapis.com"
                       }}
    end

    test "does not raise when pool is not available" do
      capture_log(fn ->
        GenServer.stop(Logflare.FinchDefault)
        GenServer.stop(Logflare.FinchIngest)
        GenServer.stop(Logflare.FinchQuery)
        assert Cluster.finch()

        Process.sleep(100)
      end) =~ ":gen_statem"
    end
  end
end
