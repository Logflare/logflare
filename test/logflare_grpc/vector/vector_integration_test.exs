defmodule LogflareGrpc.Vector.IntegrationTest do
  @moduledoc false

  # Drives a real Vector container against the Logflare gRPC endpoint to verify
  # wire-level compatibility between Vector's native `vector` sink and the
  # proto definitions/server in this repo — something the in-process stub test
  # (server_test.exs) cannot catch.
  #
  # Vector's `vector` sink cannot send `x-api-key`/`x-source` (mTLS is the only
  # auth it supports), so an nginx `grpc_pass` proxy injects those headers,
  # modeling the planned ALB/Envoy layer:
  #
  #     vector --h2c--> nginx (grpc_set_header) --h2c--> Logflare :grpc
  #
  # The ClickHouse case additionally verifies the OTEL column mapping end-to-end
  # with real Vector (log severity, metric type/value).
  #
  # Excluded from the default suite. Run explicitly (requires Docker):
  #
  #     mix test --only integration test/logflare_grpc/vector/vector_integration_test.exs
  use Logflare.DataCase, async: false

  import Phoenix.ConnTest
  import Plug.Conn

  @moduletag :integration
  # First run pulls images; CH boot + Vector connect + pipeline flush take time.
  @moduletag timeout: 360_000
  @endpoint LogflareWeb.Endpoint

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtils
  alias LogflareWeb.Router.Helpers, as: Routes

  @vector_image "timberio/vector:0.49.0-alpine"
  @nginx_image "nginx:1.27-alpine"
  @clickhouse_image "clickhouse/clickhouse-server:26.2"
  @nginx_port 6081

  setup do
    unless docker_available?() do
      raise "Docker is required for the Vector integration test but was not found or is not running"
    end

    insert(:plan)
    start_supervised!(AllLogsLogged)

    user = insert(:user)
    source = insert(:source, user: user)
    access_token = insert(:access_token, resource_owner: user, scopes: "ingest")

    {:ok, _pid, port} = GRPC.Server.start_endpoint(LogflareGrpc.Endpoint, 0)
    on_exit(fn -> GRPC.Server.stop_endpoint(LogflareGrpc.Endpoint) end)

    {:ok, %{user: user, source: source, access_token: access_token, port: port}}
  end

  describe "Postgres backend" do
    setup %{source: source} do
      repo = Application.get_env(:logflare, Logflare.Repo)

      url =
        "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

      backend =
        insert(:backend, type: :postgres, sources: [source], config: %{url: url, schema: nil})

      start_supervised!({AdaptorSupervisor, {source, backend}})
      on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

      {:ok, %{backend: backend}}
    end

    test "real Vector pushes logs into Logflare and they land in Postgres", ctx do
      marker = drive_vector(ctx, metrics?: false)
      table = PostgresAdaptor.table_name(ctx.source)

      TestUtils.retry_assert([duration: 120_000, sleep: 2_000], fn ->
        assert {:ok, %QueryResult{rows: rows}} =
                 PostgresAdaptor.execute_query(ctx.backend, "select body from #{table}", [])

        assert Enum.any?(rows, fn %{"body" => [body | _]} ->
                 body["event_message"] == marker and body["marker"] == marker
               end)
      end)
    end
  end

  describe "ClickHouse backend" do
    setup %{user: user, source: source} do
      ensure_clickhouse_running()

      {^source, backend} = setup_clickhouse_test(user: user, source: source)
      start_supervised!({ClickHouseAdaptor, backend})
      assert :ok = ClickHouseAdaptor.provision_ingest_tables(backend)

      {:ok, %{backend: backend}}
    end

    test "real Vector logs + metrics map onto OTEL ClickHouse columns", ctx do
      marker = drive_vector(ctx, metrics?: true)

      logs_table = ClickHouseAdaptor.clickhouse_ingest_table_name(ctx.backend, :log)
      metrics_table = ClickHouseAdaptor.clickhouse_ingest_table_name(ctx.backend, :metric)

      # Logs: the preserved nested `metadata.parsed.error_severity` (Supabase
      # shape) maps onto severity_text (upcased), proving the metadata object
      # survives flattening rather than being clobbered by the type marker.
      TestUtils.retry_assert([duration: 150_000, sleep: 3_000], fn ->
        assert {:ok, rows} =
                 ClickHouseAdaptor.execute_ch_query(
                   ctx.backend,
                   "SELECT event_message, severity_text FROM #{logs_table} WHERE event_message = '#{marker}'"
                 )

        assert Enum.any?(rows, &(&1["severity_text"] == "WARNING"))
      end)

      # Metrics: a real counter (e.g. vector_*_total) maps to metric_type `sum`
      # with a scalar value > 0 — proving the value/type mapping fix end-to-end.
      TestUtils.retry_assert([duration: 150_000, sleep: 3_000], fn ->
        assert {:ok, rows} =
                 ClickHouseAdaptor.execute_ch_query(
                   ctx.backend,
                   "SELECT toString(metric_type) AS metric_type, value FROM #{metrics_table} WHERE value > 0 LIMIT 200"
                 )

        assert Enum.any?(rows, &(&1["metric_type"] == "sum"))
      end)
    end
  end

  describe "HTTP-JSON vs gRPC parity" do
    setup %{user: user, source: source_a} do
      repo = Application.get_env(:logflare, Logflare.Repo)

      url =
        "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

      backend_a =
        insert(:backend, type: :postgres, sources: [source_a], config: %{url: url, schema: nil})

      start_supervised!(
        Supervisor.child_spec({AdaptorSupervisor, {source_a, backend_a}}, id: :adaptor_sup_a)
      )

      on_exit(fn -> PostgresAdaptor.destroy_instance({source_a, backend_a}) end)

      source_b = insert(:source, user: user)

      backend_b =
        insert(:backend, type: :postgres, sources: [source_b], config: %{url: url, schema: nil})

      start_supervised!(
        Supervisor.child_spec({AdaptorSupervisor, {source_b, backend_b}}, id: :adaptor_sup_b)
      )

      on_exit(fn -> PostgresAdaptor.destroy_instance({source_b, backend_b}) end)

      {:ok, %{backend_a: backend_a, source_b: source_b, backend_b: backend_b}}
    end

    test "the same event stores equivalently via the vector sink and /api/logs",
         %{source: source_a} = ctx do
      # Capture server for Vector's real `http`/json sink output.
      capture_port = free_port()

      start_supervised!(
        {Bandit,
         plug: {Logflare.Test.VectorCapturePlug, target: self()},
         scheme: :http,
         ip: {0, 0, 0, 0},
         port: capture_port}
      )

      # One real Vector instance fans the same events to two sinks: the native
      # `vector` gRPC sink (-> nginx -> source A) and an `http`/json sink
      # (-> capture server). Both carry the same marker.
      marker =
        drive_vector(ctx,
          metrics?: false,
          capture_url: "http://host.docker.internal:#{capture_port}/"
        )

      # Replay Vector's actual HTTP body byte-for-byte through the real
      # /api/logs controller (in-process) -> source B.
      assert_receive {:vector_http_body, raw_body}, 90_000
      assert [_ | _] = Jason.decode!(raw_body)

      conn =
        build_conn()
        |> put_req_header("x-api-key", ctx.user.api_key)
        |> put_req_header("content-type", "application/json")
        |> post(Routes.log_path(build_conn(), :create, source: ctx.source_b.token), raw_body)

      assert conn.status in 200..299

      body_a = await_body(ctx.backend_a, source_a, marker)
      body_b = await_body(ctx.backend_b, ctx.source_b, marker)

      # The Postgres adaptor stores nested maps as single-element records, so
      # unwrap before comparing. Both paths preserve the nested metadata
      # identically; the gRPC path only adds the routing `type` marker.
      meta_a = first_record(body_a["metadata"])
      meta_b = first_record(body_b["metadata"])

      assert body_a["event_message"] == marker
      assert body_b["event_message"] == marker

      assert meta_a["host"] == "db-default"
      assert meta_b["host"] == "db-default"

      assert first_record(meta_a["parsed"])["error_severity"] == "warning"
      assert first_record(meta_b["parsed"])["error_severity"] == "warning"

      assert meta_a["type"] == "vector_log"
      refute Map.has_key?(meta_b, "type")
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp first_record(value), do: value |> List.wrap() |> List.first() || %{}

  defp await_body(backend, source, marker) do
    table = PostgresAdaptor.table_name(source)
    query = "select body from #{table}"

    TestUtils.retry_assert([duration: 120_000, sleep: 2_000], fn ->
      assert {:ok, %QueryResult{rows: rows}} = PostgresAdaptor.execute_query(backend, query, [])
      assert find_body(rows, marker)
    end)

    {:ok, %QueryResult{rows: rows}} = PostgresAdaptor.execute_query(backend, query, [])
    find_body(rows, marker)
  end

  defp find_body(rows, marker) do
    Enum.find_value(rows, fn %{"body" => [body | _]} ->
      if body["marker"] == marker, do: body
    end)
  end

  # Sets up network + nginx (header injection) + vector containers and returns
  # the unique marker emitted by the demo logs. Cleans up on exit.
  defp drive_vector(ctx, opts) do
    metrics? = Keyword.get(opts, :metrics?, false)
    capture_url = Keyword.get(opts, :capture_url)
    marker = "vector-it-#{TestUtils.random_string(12)}"
    suffix = TestUtils.random_string(8)

    network = "logflare-it-net-#{suffix}"
    nginx = "logflare-it-nginx-#{suffix}"
    vector = "logflare-it-vector-#{suffix}"

    nginx_conf = write_nginx_config(suffix, ctx.port, ctx.access_token.token, ctx.source.token)

    vector_conf =
      write_vector_config(suffix, marker, "http://#{nginx}:#{@nginx_port}", metrics?, capture_url)

    on_exit(fn -> Enum.each([nginx_conf, vector_conf], &File.rm/1) end)

    {_, 0} = docker(["network", "create", network])
    on_exit(fn -> docker(["network", "rm", network]) end)
    on_exit(fn -> docker(["rm", "-f", nginx, vector]) end)

    assert {_, 0} =
             docker([
               "run",
               "-d",
               "--name",
               nginx,
               "--network",
               network,
               "--add-host=host.docker.internal:host-gateway",
               "-v",
               "#{nginx_conf}:/etc/nginx/nginx.conf:ro",
               @nginx_image
             ])

    assert {_, 0} =
             docker([
               "run",
               "-d",
               "--name",
               vector,
               "--network",
               network,
               "--add-host=host.docker.internal:host-gateway",
               "-v",
               "#{vector_conf}:/etc/vector/vector.toml:ro",
               @vector_image,
               "--config",
               "/etc/vector/vector.toml"
             ])

    marker
  end

  defp ensure_clickhouse_running do
    unless clickhouse_reachable?() do
      name = "logflare-it-clickhouse"
      docker(["rm", "-f", name])

      {_, 0} =
        docker([
          "run",
          "-d",
          "--name",
          name,
          "-p",
          "8123:8123",
          "-p",
          "9000:9000",
          "-e",
          "CLICKHOUSE_DB=logflare_test",
          "-e",
          "CLICKHOUSE_USER=logflare",
          "-e",
          "CLICKHOUSE_PASSWORD=logflare",
          "-e",
          "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
          @clickhouse_image
        ])

      on_exit(fn -> docker(["rm", "-f", name]) end)

      wait_for_clickhouse(60)
    end
  end

  defp wait_for_clickhouse(0), do: raise("ClickHouse did not become ready in time")

  defp wait_for_clickhouse(attempts) do
    if clickhouse_reachable?() do
      :ok
    else
      Process.sleep(1_000)
      wait_for_clickhouse(attempts - 1)
    end
  end

  defp clickhouse_reachable? do
    case System.cmd("curl", ["-s", "-m", "2", "http://localhost:8123/ping"],
           stderr_to_stdout: true
         ) do
      {out, 0} -> String.contains?(out, "Ok")
      _ -> false
    end
  end

  defp docker(args), do: System.cmd("docker", args, stderr_to_stdout: true)

  defp docker_available? do
    case System.find_executable("docker") do
      nil -> false
      _ -> match?({_, 0}, docker(["info"]))
    end
  end

  defp write_nginx_config(suffix, logflare_port, api_key, source_token) do
    path = Path.join([File.cwd!(), "test", "vector", "integration_nginx_#{suffix}.conf"])

    contents = """
    worker_processes 1;
    error_log /dev/stderr info;
    pid /tmp/nginx.pid;
    events { worker_connections 128; }
    http {
      access_log /dev/stdout;
      server {
        listen #{@nginx_port};
        http2 on;
        location / {
          grpc_pass grpc://host.docker.internal:#{logflare_port};
          grpc_set_header x-api-key #{api_key};
          grpc_set_header x-source #{source_token};
        }
      }
    }
    """

    File.write!(path, contents)
    path
  end

  defp write_vector_config(suffix, marker, address, metrics?, capture_url) do
    path = Path.join([File.cwd!(), "test", "vector", "integration_vector_#{suffix}.toml"])

    metrics_source =
      if metrics? do
        """
        [sources.metrics]
        type = "internal_metrics"
        scrape_interval_secs = 2
        """
      else
        ""
      end

    sink_inputs = if metrics?, do: ~s(["tag", "metrics"]), else: ~s(["tag"])

    # Mirrors Supabase's `http`/json sink so the test can capture Vector's real
    # HTTP serialization and replay it through Logflare's /api/logs endpoint.
    capture_sink =
      if capture_url do
        """

        [sinks.capture]
        type = "http"
        inputs = ["tag"]
        uri = "#{capture_url}"
        method = "post"
        encoding.codec = "json"
        """
      else
        ""
      end

    contents = """
    data_dir = "/tmp/vector-data"

    [sources.demo]
    type = "demo_logs"
    format = "shuffle"
    lines = ["#{marker}"]
    count = 10
    interval = 0.1
    #{metrics_source}
    [transforms.tag]
    type = "remap"
    inputs = ["demo"]
    source = '''
    .event_message = .message
    .marker = "#{marker}"
    # Mirrors Supabase's metadata-centric shape (e.g. db_logs sets
    # .metadata.parsed.error_severity).
    .metadata = {"host": "db-default", "parsed": {"error_severity": "warning"}}
    '''

    [sinks.logflare]
    type = "vector"
    inputs = #{sink_inputs}
    address = "#{address}"
    compression = true
    #{capture_sink}
    """

    File.write!(path, contents)
    path
  end
end
