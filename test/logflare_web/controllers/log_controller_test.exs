defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.SingleTenant
  alias Logflare.Users
  alias Logflare.Sources.Source.V1SourceSup
  alias Logflare.Sources
  alias Logflare.SystemMetrics.AllLogsLogged

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @valid %{"some" => "valid log entry", "event_message" => "hi!"}
  @valid_json Jason.encode!(@valid)
  @invalid %{"some" => {123, "invalid"}, 123 => "hi!", 1 => :invalid}
  @valid_batch [
    %{"some" => "valid log entry", "event_message" => "hi!"},
    %{"some" => "valid log entry 2", "event_message" => "hi again!"}
  ]
  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "v2 pipeline" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id, v2_pipeline: true)
      _plan = insert(:plan, name: "Free")

      backend =
        insert(:backend, sources: [source], type: :webhook, config: %{url: "some url"})

      {:ok, source: source, user: user, backend: backend}
    end

    setup [:warm_caches, :reject_context_functions]

    test "valid ingestion", %{conn: conn, source: source, user: user} do
      pid = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn _req ->
        send(pid, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}

      TestUtils.retry_assert(fn ->
        assert_received ^ref
      end)
    end

    test "valid ingestion using ?collection=", %{conn: conn, source: source, user: user} do
      pid = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn _req ->
        send(pid, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, collection: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}

      TestUtils.retry_assert(fn ->
        assert_received ^ref
      end)
    end

    test "valid ingestion using ?collection_name=", %{conn: conn, source: source, user: user} do
      pid = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn _req ->
        send(pid, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, collection_name: source.name), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}

      TestUtils.retry_assert(fn ->
        assert_received ^ref
      end)
    end

    test ":cloud_event ingestion", %{conn: conn, source: source, user: user} do
      this = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("ce-foo-foo", "bar")
        |> post(Routes.log_path(conn, :cloud_event, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert_receive {^ref, [log]}, 3000

      assert %{"some" => _, "event_message" => _, "cloud_event" => %{"foo_foo" => "bar"}} =
               log
    end

    test ":otel_traces ingestion", %{conn: conn, source: source, user: user} do
      this = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      body = TestUtilsGrpc.random_export_service_request() |> ExportTraceServiceRequest.encode()

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("x-source", Atom.to_string(source.token))
        |> put_req_header("content-type", "application/x-protobuf")
        |> post(Routes.log_path(conn, :otel_traces), body)

      assert protobuf_response(conn, 200, ExportTraceServiceResponse) ==
               %ExportTraceServiceResponse{partial_success: nil}

      assert_receive {^ref, [event1, event2]}, 4000

      assert event1["trace_id"] == event2["trace_id"]
      assert %{"metadata" => _, "event_message" => _} = event1
      assert %{"metadata" => _, "event_message" => _} = event2
    end

    test ":otel_metrics ingestion", %{conn: conn, source: source, user: user} do
      this = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      body =
        TestUtilsGrpc.random_otel_metrics_request()
        |> ExportMetricsServiceRequest.encode()

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("x-source", Atom.to_string(source.token))
        |> put_req_header("content-type", "application/x-protobuf")
        |> post(Routes.log_path(conn, :otel_metrics), body)

      assert protobuf_response(conn, 200, ExportMetricsServiceResponse) ==
               %ExportMetricsServiceResponse{partial_success: nil}

      assert_receive {^ref, data_points}, 4000

      Enum.each(data_points, fn data_point ->
        assert %{"metadata" => _, "event_message" => _, "metric_type" => _} = data_point
      end)
    end

    test ":otel_logs ingestion", %{conn: conn, source: source, user: user} do
      this = self()
      ref = make_ref()

      WebhookAdaptor.Client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      body =
        TestUtilsGrpc.random_otel_logs_request()
        |> ExportLogsServiceRequest.encode()

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("x-source", Atom.to_string(source.token))
        |> put_req_header("content-type", "application/x-protobuf")
        |> post(Routes.log_path(conn, :otel_logs), body)

      assert protobuf_response(conn, 200, ExportLogsServiceResponse) ==
               %ExportLogsServiceResponse{partial_success: nil}

      assert_receive {^ref, logs}, 4000

      Enum.each(logs, fn log ->
        assert %{
                 "metadata" => %{"type" => "otel_log"},
                 "event_message" => _,
                 "attributes" => _,
                 "timestamp" => _
               } = log
      end)
    end

    test "invaild source token uuid checks", %{conn: conn, user: user} do
      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> post(Routes.log_path(conn, :create, source: ":signin"), @valid)

      assert json_response(conn, 401)
    end
  end

  describe "v1 pipeline invalid" do
    setup [:v1_pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)

      {:ok, user: user, conn: conn}
    end

    test ":create rejected logs ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @invalid)

      assert %{"message" => [msg]} = json_response(conn, 406)
      assert msg =~ "not supported by"
    end

    test ":create ingestion error handling for BadRequestError in Plug.Parsers", %{
      conn: conn,
      source: source
    } do
      assert_raise LogflareWeb.JsonParser.BadRequestError, fn ->
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("content-encoding", "br")
        |> post(
          Routes.log_path(conn, :create, source: source.token),
          Jason.encode!(%{"batch" => @valid_batch})
        )
      end
    end
  end

  describe "v1 pipeline with legacy user.api_key" do
    setup [:v1_pipeline_setup, :warm_caches, :reject_context_functions, :expect_broadcast]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)

      {:ok, user: user, conn: conn}
    end

    test ":create ingestion by source_name", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion with gzip", %{conn: conn, source: source} do
      batch = for _i <- 1..1500, do: @valid
      payload = :zlib.gzip(Jason.encode!(%{"batch" => batch}))

      conn =
        conn
        |> Plug.Conn.put_req_header("content-encoding", "gzip")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> post(~p"/logs?#{[source: source.token]}", payload)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion batch with `batch` key", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), %{"batch" => @valid_batch})

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":create ingestion batch with array body", %{conn: conn, source: source} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(Routes.log_path(conn, :create, source: source.token), Jason.encode!(@valid_batch))

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end

    test ":cloudflare ingestion", %{conn: new_conn, source: source} do
      path = Routes.log_path(new_conn, :cloudflare, source: source.token)

      assert new_conn |> post(path, @valid) |> json_response(200) == %{"message" => "Logged!"}

      assert new_conn
             |> post(path, %{batch: [@valid]})
             |> json_response(200) == %{
               "message" => "Logged!"
             }

      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end
  end

  describe "v1 pipeline with access tokens" do
    setup [:v1_pipeline_setup, :expect_broadcast]

    setup %{user: user, conn: conn} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user)
      conn = put_req_header(conn, "x-api-key", access_token.token)
      {:ok, user: user, conn: conn}
    end

    setup [:warm_caches, :reject_context_functions]

    test ":create ingestion by source_name", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(2000)
    end
  end

  describe "v1 pipeline no broadcast expectation" do
    setup [:v1_pipeline_setup]

    setup %{user: user, conn: conn} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user)
      conn = put_req_header(conn, "x-api-key", access_token.token)
      {:ok, user: user, conn: conn}
    end

    setup [:warm_caches, :reject_context_functions]

    test ":cloud_event ingestion", %{conn: conn, source: source, user: user} do
      this = self()
      ref = make_ref()

      Logflare.Google.BigQuery
      |> expect(:stream_batch!, fn _, batch ->
        [%{json: json}] = batch
        send(this, {ref, json})
        {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("ce-foo-foo", "bar")
        |> post(Routes.log_path(conn, :cloud_event, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}
      assert_receive {^ref, log}, 3000

      assert %{
               "some" => _,
               "event_message" => _,
               "id" => _,
               "cloud_event" => [%{"foo_foo" => "bar"}]
             } = log
    end
  end

  describe "single tenant" do
    TestUtils.setup_single_tenant(seed_user: true)

    setup %{conn: conn} do
      # get single tenant user
      user = SingleTenant.get_default_user()

      # insert the source
      source = insert(:source, user: user)

      # ingestion setup
      start_supervised!({V1SourceSup, source: source})
      :timer.sleep(500)

      Logflare.Logs
      |> expect(:broadcast, 1, fn le ->
        le
      end)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)

      [source: source, conn: conn]
    end

    test ":create ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert json_response(conn, 200) == %{"message" => "Logged!"}

      # wait for all logs to be ingested before removing all stubs
      :timer.sleep(3000)
    end
  end

  defp v1_pipeline_setup(%{conn: conn}) do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({V1SourceSup, source: source})
    :timer.sleep(500)

    Logflare.Logs
    |> stub(:broadcast, fn le -> le end)

    {:ok, source: source, user: user, conn: conn}
  end

  defp expect_broadcast(_) do
    Logflare.Logs
    |> expect(:broadcast, 1, fn le ->
      assert match?(@valid, le.body)
      assert le.body["event_message"] != nil
      assert Map.keys(le.body) |> length() == 4, inspect(Map.keys(le.body))

      le
    end)

    :ok
  end

  defp warm_caches(%{user: user, source: source}) do
    # hit the caches
    Sources.Cache.get_by_and_preload_rules(token: Atom.to_string(source.token))
    Sources.Cache.get_by_and_preload_rules(name: source.name, user_id: user.id)
    Sources.Cache.get_source_by_token(source.token)
    Sources.Cache.get_by_id(source.id)
    Users.Cache.get(user.id)
    Users.Cache.get_by(id: user.id)
    Users.Cache.get_by(api_key: user.api_key)
    Users.Cache.preload_defaults(user)
    Users.Cache.get(user.id)

    on_exit(fn ->
      Cachex.clear(Users.Cache)
      Cachex.clear(Sources.Cache)
    end)

    :ok
  end

  defp reject_context_functions(_) do
    reject(&Sources.get_source_by_token/1)
    reject(&Sources.get/1)
    # Allow Sources.get_by/1 to be called by background processes (like the SourceSupWorker)
    # but stub it to return nil to avoid actual database calls
    stub(Sources, :get_by, fn _ -> nil end)
    reject(&Sources.get_by_and_preload_rules/1)
    reject(&Sources.preload_defaults/1)
    reject(&Users.get/1)
    reject(&Users.get_by/1)
    reject(&Users.get_by_and_preload/1)
    reject(&Users.preload_team/1)
    reject(&Users.preload_billing_account/1)
    :ok
  end

  describe "benchmarks" do
    setup [:v1_pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)
      {:ok, user: user, conn: conn}
    end

    @tag :benchmark
    test "v1 ingestion", %{conn: conn, source: source} do
      Logflare.Logs
      |> stub(:ingest, fn _ -> :ok end)

      batch = Jason.encode!(%{"batch" => for(_ <- 1..250, do: @valid)})

      big_single =
        for i <- 1..250, into: %{} do
          {"key_#{i}", TestUtils.random_string()}
        end

      Benchee.run(
        %{
          "single" => fn -> do_ingest_post(conn, source, @valid_json) end,
          "big_single" => fn -> do_ingest_post(conn, source, big_single) end,
          "batched" => fn -> do_ingest_post(conn, source, batch) end
        },
        time: 2,
        warmup: 1,
        memory_time: 1,
        reduction_time: 1
      )
    end
  end

  defp do_ingest_post(conn, source, input) do
    conn
    |> put_req_header("content-type", "application/json")
    |> post(Routes.log_path(conn, :create, source_name: source.name), input)
  end

  defp protobuf_response(conn, expected_status, protobuf_schema) do
    body = Phoenix.ConnTest.response(conn, expected_status)

    protobuf_schema.decode(body)
  end
end
