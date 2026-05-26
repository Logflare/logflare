defmodule LogflareWeb.LogControllerTest do
  @moduledoc false
  use LogflareWeb.ConnCase
  import ExUnit.CaptureLog
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.SingleTenant
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @valid %{"some" => "valid log entry", "event_message" => "hi!"}
  @valid_json Jason.encode!(@valid)
  @valid_batch [
    %{"some" => "valid log entry", "event_message" => "hi!"},
    %{"some" => "valid log entry 2", "event_message" => "hi again!"}
  ]
  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "log ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      _plan = insert(:plan, name: "Free")

      backend =
        insert(:backend, sources: [source], type: :webhook, config: %{url: "some url"})

      {:ok, source: source, user: user, backend: backend}
    end

    setup [:warm_caches, :reject_context_functions]

    for {param_name, param_key} <- [
          {"source", :source},
          {"collection", :collection},
          {"collection_name", :collection_name}
        ] do
      test "valid ingestion using ?#{param_name}=", %{conn: conn, source: source, user: user} do
        {_pid, ref} = expect_webhook_success()

        value =
          case unquote(param_key) do
            :collection_name -> source.name
            _ -> source.token
          end

        conn =
          conn
          |> put_req_header("x-api-key", user.api_key)
          |> post(Routes.log_path(conn, :create, [{unquote(param_key), value}]), @valid)

        assert_logged_successfully(conn)
        assert_eventually_received(ref)
      end
    end

    test ":cloud_event ingestion", %{conn: conn, source: source, user: user} do
      {_this, ref} = expect_webhook_with_body()

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("ce-foo-foo", "bar")
        |> post(Routes.log_path(conn, :cloud_event, source: source.token), @valid)

      assert_logged_successfully(conn)
      assert_receive {^ref, [log]}, 3000

      assert %{"some" => _, "event_message" => _, "cloud_event" => %{"foo_foo" => "bar"}} =
               log
    end

    test ":otel_traces ingestion", %{conn: conn, source: source, user: user} do
      {_this, ref} = expect_webhook_with_body()

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
      {_this, ref} = expect_webhook_with_body()

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

    for {path, request_module, response_module} <- [
          {:otel_metrics, ExportMetricsServiceRequest, ExportMetricsServiceResponse},
          {:otel_traces, ExportTraceServiceRequest, ExportTraceServiceResponse},
          {:otel_logs, ExportLogsServiceRequest, ExportLogsServiceResponse}
        ] do
      test "#{path} returns 200 (not 500) when validation rejects events", %{
        conn: conn,
        source: source,
        user: user
      } do
        Mimic.stub(Logflare.Backends, :ingest_logs, fn _batch, _source ->
          {:error, ["Type error! Field `value` has an unexpected type."]}
        end)

        body =
          case unquote(path) do
            :otel_metrics -> TestUtilsGrpc.random_otel_metrics_request()
            :otel_traces -> TestUtilsGrpc.random_export_service_request()
            :otel_logs -> TestUtilsGrpc.random_otel_logs_request()
          end
          |> unquote(request_module).encode()

        log =
          capture_log(fn ->
            conn =
              conn
              |> put_req_header("x-api-key", user.api_key)
              |> put_req_header("x-source", Atom.to_string(source.token))
              |> put_req_header("content-type", "application/x-protobuf")
              |> post(Routes.log_path(conn, unquote(path)), body)

            assert protobuf_response(conn, 200, unquote(response_module)) ==
                     struct!(unquote(response_module), partial_success: nil)
          end)

        assert log =~ "OTLP ingest rejected"
      end
    end

    test ":otel_logs ingestion", %{conn: conn, source: source, user: user} do
      {_this, ref} = expect_webhook_with_body()

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

  describe "system sources" do
    test "gets unauthorized message", %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user, system_source: true)
      insert(:plan, name: "Free")

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("x-source", Atom.to_string(source.token))
        |> put_req_header("content-type", "application/x-protobuf")
        |> post(Routes.log_path(conn, :otel_metrics), @valid)

      assert json_response(conn, 401)
    end
  end

  # The syslog Plug.Parsers parser used to run before auth, so an
  # unauthenticated POST with a malformed payload could write attacker-supplied
  # content to application logs via Logger.error. Auth now runs ahead of body
  # parsing on ingest pipelines, and parser error logs no longer include the
  # raw body either.
  describe "ingest auth runs before parsers" do
    @attacker_marker "MALICIOUS_LOG_INJECTION_MARKER_2026"
    @malformed_syslog "not a valid syslog message #{@attacker_marker}\n"
    @malformed_ndjson "{not json #{@attacker_marker}\n"

    test "unauthenticated /logs/logplex returns 401 without parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/logplex-1")
            |> post(~p"/logs/logplex", @malformed_syslog)

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
      refute log =~ "Syslog message parsing error"
    end

    test "unauthenticated /logs/cloudflare returns 401 without parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/x-ndjson")
            |> post(~p"/logs/cloudflare", @malformed_ndjson)

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
      refute log =~ "NDJSON parser error"
    end

    test "unauthenticated /v1/logs returns 401 without parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/x-protobuf")
            |> post(~p"/v1/logs", "garbage protobuf #{@attacker_marker}")

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
    end

    test "invalid api key on /logs/logplex 401s before parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/logplex-1")
            |> put_req_header("x-api-key", "this-key-is-not-valid")
            |> post(~p"/logs/logplex", @malformed_syslog)

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
      refute log =~ "Syslog message parsing error"
    end

    test "invalid api key on /logs/cloudflare 401s before parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/x-ndjson")
            |> put_req_header("x-api-key", "this-key-is-not-valid")
            |> post(~p"/logs/cloudflare", @malformed_ndjson)

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
      refute log =~ "NDJSON parser error"
    end

    test "invalid api key on /v1/logs 401s before parsing the body", %{conn: conn} do
      log =
        capture_log(fn ->
          conn =
            conn
            |> put_req_header("content-type", "application/x-protobuf")
            |> put_req_header("x-api-key", "this-key-is-not-valid")
            |> post(~p"/v1/logs", "garbage protobuf #{@attacker_marker}")

          assert json_response(conn, 401)
        end)

      refute log =~ @attacker_marker
    end

    test "authenticated /logs/logplex with malformed body does not log raw content", %{conn: conn} do
      user = insert(:user)
      source = insert(:source, user: user)
      insert(:plan, name: "Free")

      log =
        capture_log(fn ->
          conn
          |> put_req_header("x-api-key", user.api_key)
          |> put_req_header("content-type", "application/logplex-1")
          |> post(~p"/logs/logplex?#{[source: source.token]}", @malformed_syslog)
        end)

      assert log =~ "Syslog message parsing error"
      refute log =~ @attacker_marker
    end

    test "authenticated /logs/cloudflare with malformed body does not log raw content", %{
      conn: conn
    } do
      user = insert(:user)
      source = insert(:source, user: user)
      insert(:plan, name: "Free")

      log =
        capture_log(fn ->
          conn
          |> put_req_header("x-api-key", user.api_key)
          |> put_req_header("content-type", "application/x-ndjson")
          |> post(~p"/logs/cloudflare?#{[source: source.token]}", @malformed_ndjson)
        end)

      assert log =~ "NDJSON parser error"
      refute log =~ @attacker_marker
    end

    test "authenticated /logs/logplex error log does not include ?source= query param",
         %{conn: conn} do
      user = insert(:user)
      insert(:plan, name: "Free")

      log =
        capture_log(fn ->
          conn
          |> put_req_header("x-api-key", user.api_key)
          |> put_req_header("content-type", "application/logplex-1")
          |> post(~p"/logs/logplex?#{[source: @attacker_marker]}", @malformed_syslog)
        end)

      assert log =~ "Syslog message parsing error"
      refute log =~ @attacker_marker
    end
  end

  describe "pipeline with ?api_key= query auth" do
    setup [:pipeline_setup, :warm_caches, :reject_context_functions]

    test ":create ingestion authenticates via ?api_key=", %{
      conn: conn,
      source: source,
      user: user
    } do
      expect_bq_insert()

      conn =
        post(
          conn,
          ~p"/logs?#{[source: source.token, api_key: user.api_key]}",
          @valid
        )

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end
  end

  describe "pipeline invalid" do
    setup [:pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)

      {:ok, user: user, conn: conn}
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

  describe "pipeline with legacy user.api_key" do
    setup [:pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)

      {:ok, user: user, conn: conn}
    end

    test ":create ingestion by source_name", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion with gzip", %{conn: conn, source: source} do
      expect_bq_insert()

      batch = for _i <- 1..100, do: @valid
      payload = :zlib.gzip(Jason.encode!(%{"batch" => batch}))

      conn =
        conn
        |> Plug.Conn.put_req_header("content-encoding", "gzip")
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> post(~p"/logs?#{[source: source.token]}", payload)

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion batch with `batch` key", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), %{"batch" => @valid_batch})

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion batch with array body", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(Routes.log_path(conn, :create, source: source.token), Jason.encode!(@valid_batch))

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":cloudflare ingestion", %{conn: new_conn, source: source} do
      expect_bq_insert()

      path = Routes.log_path(new_conn, :cloudflare, source: source.token)

      assert new_conn |> post(path, @valid) |> json_response(200) == %{"message" => "Logged!"}

      assert new_conn
             |> post(path, %{batch: [@valid]})
             |> json_response(200) == %{
               "message" => "Logged!"
             }

      assert_eventually_received(:inserted)
    end

    test ":elixir_logger ingestion with source_name in BERT body", %{conn: conn, source: source} do
      expect_bq_insert()

      body =
        Bertex.encode(%{
          "source_name" => source.name,
          "batch" => [
            %{
              "level" => "info",
              "message" => "bert body source_name test",
              "metadata" => %{},
              "timestamp" => DateTime.to_iso8601(DateTime.utc_now())
            }
          ]
        })

      conn =
        conn
        |> put_req_header("content-type", "application/bert")
        |> post(~p"/logs/elixir/logger", body)

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion with source token in JSON body", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(
          ~p"/logs",
          Jason.encode!(%{"source" => Atom.to_string(source.token), "event_message" => "x"})
        )

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end

    test ":create ingestion with source_name in JSON body", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> post(~p"/logs", Jason.encode!(%{"source_name" => source.name, "event_message" => "x"}))

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end
  end

  describe "pipeline with access tokens" do
    setup [:pipeline_setup]

    setup %{user: user, conn: conn} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user)
      conn = put_req_header(conn, "x-api-key", access_token.token)
      {:ok, user: user, conn: conn}
    end

    setup [:warm_caches, :reject_context_functions]

    test ":create ingestion by source_name", %{conn: conn, source: source} do
      expect_bq_insert()

      conn =
        conn
        |> post(Routes.log_path(conn, :create, source_name: source.name), @valid)

      assert_logged_successfully(conn)
      assert_eventually_received(:inserted)
    end
  end

  describe "pipeline no broadcast expectation" do
    setup [:pipeline_setup]

    setup %{user: user, conn: conn} do
      {:ok, access_token} = Logflare.Auth.create_access_token(user)
      conn = put_req_header(conn, "x-api-key", access_token.token)
      {:ok, user: user, conn: conn}
    end

    setup [:warm_caches, :reject_context_functions]

    test ":cloud_event ingestion", %{conn: conn, source: source, user: user} do
      {_this, ref} = expect_bq_stream_with_body()

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)
        |> put_req_header("ce-foo-foo", "bar")
        |> post(Routes.log_path(conn, :cloud_event, source: source.token), @valid)

      assert_logged_successfully(conn)
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
      start_supervised!({SourceSup, source})
      :timer.sleep(500)

      conn =
        conn
        |> put_req_header("x-api-key", user.api_key)

      [source: source, conn: conn]
    end

    test ":create ingestion", %{conn: conn, source: source} do
      conn =
        conn
        |> post(Routes.log_path(conn, :create, source: source.token), @valid)

      assert_logged_successfully(conn)

      :timer.sleep(3000)
    end
  end

  defp pipeline_setup(%{conn: conn}) do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({SourceSup, source})
    :timer.sleep(500)

    {:ok, source: source, user: user, conn: conn}
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
    setup [:pipeline_setup, :warm_caches, :reject_context_functions]

    setup %{user: user, conn: conn} do
      conn = put_req_header(conn, "x-api-key", user.api_key)
      {:ok, user: user, conn: conn}
    end

    @tag :benchmark
    test "ingestion", %{conn: conn, source: source} do
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

  defp expect_webhook_success(pid \\ self(), ref \\ make_ref()) do
    WebhookAdaptor.Client
    |> expect(:send, fn _req ->
      send(pid, ref)
      %Tesla.Env{status: 200, body: ""}
    end)

    {pid, ref}
  end

  defp expect_webhook_with_body(pid \\ self(), ref \\ make_ref()) do
    WebhookAdaptor.Client
    |> expect(:send, fn req ->
      send(pid, {ref, req[:body]})
      %Tesla.Env{status: 200, body: ""}
    end)

    {pid, ref}
  end

  defp expect_bq_insert(pid \\ self()) do
    GoogleApi.BigQuery.V2.Api.Tabledata
    |> expect(:bigquery_tabledata_insert_all, fn _conn,
                                                 _project_id,
                                                 _dataset_id,
                                                 _table_name,
                                                 _opts ->
      send(pid, :inserted)
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)
  end

  defp expect_bq_stream_with_body(pid \\ self(), ref \\ make_ref()) do
    Logflare.Google.BigQuery
    |> expect(:stream_batch!, fn _, batch ->
      [%{json: json}] = batch
      send(pid, {ref, json})
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    {pid, ref}
  end

  defp assert_logged_successfully(conn) do
    assert json_response(conn, 200) == %{"message" => "Logged!"}
  end

  defp assert_eventually_received(message, timeout \\ 3000) do
    TestUtils.retry_assert([duration: timeout], fn ->
      assert_received ^message
    end)
  end
end
