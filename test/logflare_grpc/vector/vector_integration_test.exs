defmodule LogflareGrpc.Vector.IntegrationTest do
  @moduledoc false

  # Drives a real Vector container against the Logflare gRPC endpoint to verify
  # wire-level compatibility between Vector's native `vector` sink and the
  # proto definitions/server in this repo — something the in-process stub test
  # (server_test.exs) cannot catch.
  #
  # Vector's `vector` sink cannot send `x-api-key`/`x-source` (mTLS is the only
  # auth it supports), so an nginx `grpc_pass` proxy injects those headers,
  # modeling the planned ALB/Envoy layer. Topology:
  #
  #     vector --(h2c)--> nginx (grpc_set_header) --(h2c)--> Logflare :grpc
  #
  # Excluded from the default suite. Run explicitly (requires Docker):
  #
  #     mix test --only integration test/logflare_grpc/vector/vector_integration_test.exs
  use Logflare.DataCase, async: false

  @moduletag :integration
  # First run pulls the Vector + nginx images and Vector must connect + flush.
  @moduletag timeout: 240_000

  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.TestUtils

  @vector_image "timberio/vector:0.49.0-alpine"
  @nginx_image "nginx:1.27-alpine"
  @nginx_port 6081

  setup do
    unless docker_available?() do
      raise "Docker is required for the Vector integration test but was not found or is not running"
    end

    insert(:plan)
    start_supervised!(AllLogsLogged)

    user = insert(:user)
    source = insert(:source, user: user)

    repo = Application.get_env(:logflare, Logflare.Repo)

    url =
      "postgresql://#{repo[:username]}:#{repo[:password]}@#{repo[:hostname]}/#{repo[:database]}"

    backend =
      insert(:backend, type: :postgres, sources: [source], config: %{url: url, schema: nil})

    start_supervised!({AdaptorSupervisor, {source, backend}})
    on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

    {:ok, _pid, port} = GRPC.Server.start_endpoint(LogflareGrpc.Endpoint, 0)
    on_exit(fn -> GRPC.Server.stop_endpoint(LogflareGrpc.Endpoint) end)

    {:ok, %{source: source, user: user, backend: backend, port: port}}
  end

  test "real Vector pushes log events into Logflare through a header-injecting proxy", %{
    source: source,
    user: user,
    backend: backend,
    port: port
  } do
    access_token = insert(:access_token, resource_owner: user, scopes: "ingest")
    marker = "vector-it-#{TestUtils.random_string(12)}"
    suffix = TestUtils.random_string(8)

    network = "logflare-it-net-#{suffix}"
    nginx = "logflare-it-nginx-#{suffix}"
    vector = "logflare-it-vector-#{suffix}"

    nginx_conf = write_nginx_config(suffix, port, access_token.token, source.token)
    vector_conf = write_vector_config(suffix, marker, "http://#{nginx}:#{@nginx_port}")
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
               "-v",
               "#{vector_conf}:/etc/vector/vector.toml:ro",
               @vector_image,
               "--config",
               "/etc/vector/vector.toml"
             ])

    table = PostgresAdaptor.table_name(source)

    TestUtils.retry_assert([duration: 120_000, sleep: 2_000], fn ->
      assert {:ok, %QueryResult{rows: rows}} =
               PostgresAdaptor.execute_query(backend, "select body from #{table}", [])

      assert Enum.any?(rows, fn %{"body" => [body | _]} ->
               body["event_message"] == marker and body["marker"] == marker
             end),
             """
             no row with marker #{marker} yet.
             == vector logs ==
             #{container_logs(vector)}
             == nginx logs ==
             #{container_logs(nginx)}
             """
    end)
  end

  defp docker(args), do: System.cmd("docker", args, stderr_to_stdout: true)

  defp docker_available? do
    case System.find_executable("docker") do
      nil -> false
      _ -> match?({_, 0}, docker(["info"]))
    end
  end

  defp container_logs(name) do
    case docker(["logs", name]) do
      {logs, _} -> logs
      _ -> "(unavailable)"
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

  defp write_vector_config(suffix, marker, address) do
    path = Path.join([File.cwd!(), "test", "vector", "integration_vector_#{suffix}.toml"])

    contents = """
    data_dir = "/tmp/vector-data"

    [sources.demo]
    type = "demo_logs"
    format = "shuffle"
    lines = ["#{marker}"]
    count = 10
    interval = 0.1

    [transforms.tag]
    type = "remap"
    inputs = ["demo"]
    source = '''
    .event_message = .message
    .marker = "#{marker}"
    .level = "info"
    '''

    [sinks.logflare]
    type = "vector"
    inputs = ["tag"]
    address = "#{address}"
    compression = true
    """

    File.write!(path, contents)
    path
  end
end
