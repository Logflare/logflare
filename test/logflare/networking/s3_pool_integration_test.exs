defmodule Logflare.Networking.S3PoolIntegrationTest do
  @behaviour Plug

  use ExUnit.Case, async: false

  alias Logflare.Backends.Adaptor.S3Adaptor.HttpClient
  alias Logflare.Networking
  alias Logflare.SystemMetrics.Cluster
  alias Logflare.TestUtils

  setup do
    TestUtils.attach_forwarder([:finch, :conn_max_idle_time_exceeded])
    event = [:logflare, :system, :finch]
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn event, measurements, metadata, parent ->
          if self() == parent do
            send(parent, {:telemetry_event, event, measurements, metadata})
          end
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
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
  end

  test "collects real S3 origin capacity and in-flight occupancy, then clears removed origins" do
    baseline = s3_measurements()
    first_port = start_http_server()
    second_port = start_http_server()
    capacity = Keyword.get(drain_config(), :size, 50) * Keyword.get(drain_config(), :count, 1)

    for port <- [first_port, second_port] do
      get(port)
      assert_receive {:http_request, _, _, "/"}
      assert {:ok, [_ | _]} = Finch.get_pool_status(Logflare.FinchS3, origin(port))
    end

    idle = %{
      baseline
      | available_connections: baseline.available_connections + capacity * 2
    }

    assert s3_measurements() == idle

    request = Task.async(fn -> get(first_port, "/hold") end)
    assert_receive {:http_request, server, _, "/hold"}, 2_000

    assert s3_measurements() == %{
             idle
             | in_flight_requests: idle.in_flight_requests + 1,
               in_use_connections: idle.in_use_connections + 1,
               available_connections: idle.available_connections - 1
           }

    send(server, :release)
    assert :ok = Task.await(request)
    assert s3_measurements() == idle

    assert :ok = Finch.stop_pool(Logflare.FinchS3, origin(second_port))

    TestUtils.retry_assert(fn ->
      assert s3_measurements() == %{
               baseline
               | available_connections: baseline.available_connections + capacity
             }
    end)

    assert :ok = Finch.stop_pool(Logflare.FinchS3, origin(first_port))
    TestUtils.retry_assert(fn -> assert s3_measurements() == baseline end)
  end

  @impl Plug
  @spec init(pid()) :: pid()
  def init(parent), do: parent

  @impl Plug
  @spec call(Plug.Conn.t(), pid()) :: Plug.Conn.t()
  def call(conn, parent) do
    send(parent, {:http_request, self(), Plug.Conn.get_peer_data(conn).port, conn.request_path})

    if conn.request_path == "/hold" do
      receive do
        :release -> :ok
      after
        10_000 -> raise "held request was not released"
      end
    end

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

  @spec get(pos_integer(), String.t()) :: :ok
  defp get(port, path \\ "/") do
    assert {:ok, %{status_code: 200, body: "ok"}} =
             HttpClient.request(:get, "http://127.0.0.1:#{port}#{path}", "", [],
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

  @spec s3_measurements() :: map()
  defp s3_measurements do
    Cluster.finch()

    assert_receive {:telemetry_event, [:logflare, :system, :finch], measurements,
                    %{pool: "Elixir.Logflare.FinchS3", url: "all"}}

    measurements
  end
end
