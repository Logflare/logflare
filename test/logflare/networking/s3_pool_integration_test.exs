defmodule Logflare.Networking.S3PoolIntegrationTest do
  @behaviour Plug

  use ExUnit.Case, async: false

  alias Logflare.Backends.Adaptor.S3Adaptor.HttpClient
  alias Logflare.Networking
  alias Logflare.Telemetry
  alias Logflare.TestUtils
  alias OtelMetricExporter.MetricStore

  @exporter :logflare_s3_pool_idle_expiry_metrics_test

  setup do
    TestUtils.attach_forwarder([:finch, :conn_max_idle_time_exceeded])

    metric =
      Enum.find(Telemetry.metrics(), &(&1.event_name == [:finch, :conn_max_idle_time_exceeded]))

    start_supervised!(
      {OtelMetricExporter,
       name: @exporter,
       metrics: [metric],
       export_period: :timer.minutes(5),
       otlp_protocol: :http_protobuf,
       otlp_endpoint: "http://127.0.0.1:4318",
       otlp_headers: %{},
       otlp_compression: nil}
    )

    :ok
  end

  test "reuses a drain connection before the configured idle limit and replaces it afterward" do
    port = start_http_server()
    idle_limit = drain_config() |> Keyword.fetch!(:conn_max_idle_time)
    assert idle_limit == 5_000

    get(port)
    assert_receive {:http_request, _, first_peer_port, "/"}

    get(port)
    assert_receive {:http_request, _, ^first_peer_port, "/"}
    refute_received {:telemetry_event, [:finch, :conn_max_idle_time_exceeded], _, %{port: ^port}}

    Process.sleep(idle_limit + 1)

    get(port)
    assert_receive {:http_request, _, next_peer_port, "/"}
    refute next_peer_port == first_peer_port

    assert_receive {:telemetry_event, [:finch, :conn_max_idle_time_exceeded],
                    %{idle_time: idle_time}, %{scheme: :http, host: "127.0.0.1", port: ^port}}

    assert System.convert_time_unit(idle_time, :native, :millisecond) > idle_limit

    # Emitted from the drain pool's process, so the endpoint-tagged counter leaves it out.
    assert MetricStore.get_metrics(@exporter)[
             {:counter, "finch.conn_max_idle_time_exceeded.idle_time"}
           ] in [nil, %{}]
  end

  @impl Plug
  @spec init(pid()) :: pid()
  def init(parent), do: parent

  @impl Plug
  @spec call(Plug.Conn.t(), pid()) :: Plug.Conn.t()
  def call(conn, parent) do
    send(parent, {:http_request, self(), Plug.Conn.get_peer_data(conn).port, conn.request_path})

    Plug.Conn.send_resp(conn, 200, "ok")
  end

  @spec start_http_server() :: pos_integer()
  defp start_http_server do
    ref = make_ref()

    start_supervised!(
      {Plug.Cowboy,
       scheme: :http,
       plug: {__MODULE__, self()},
       options: [
         ref: ref,
         ip: {127, 0, 0, 1},
         port: 0,
         protocol_options: [idle_timeout: 60_000, request_timeout: 60_000]
       ]}
    )

    port = :ranch.get_port(ref)
    on_exit(fn -> Finch.stop_pool(Logflare.FinchS3, origin(port)) end)
    port
  end

  @spec drain_config() :: keyword()
  defp drain_config do
    {Finch, opts} =
      Enum.find(Networking.pools(), fn
        {Finch, opts} -> opts[:name] == Logflare.FinchS3
        _ -> false
      end)

    Keyword.fetch!(opts, :pools).default
  end

  @spec get(pos_integer()) :: :ok
  defp get(port) do
    assert {:ok, %{status_code: 200, body: "ok"}} =
             HttpClient.request(:get, "http://127.0.0.1:#{port}/", "", [],
               pool_timeout: 5_000,
               receive_timeout: 5_000
             )

    for {pool, Finch.HTTP1.Pool} <- Registry.lookup(Logflare.FinchS3, origin(port)) do
      :sys.get_state(pool)
    end

    :ok
  end

  @spec origin(pos_integer()) :: Finch.scheme_host_port()
  defp origin(port), do: {:http, "127.0.0.1", port}
end
