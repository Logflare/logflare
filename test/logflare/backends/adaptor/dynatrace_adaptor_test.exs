defmodule Logflare.Backends.Adaptor.DynatraceAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.DynatraceAdaptor.LogRecordFormatter
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.SourceSup
  alias Logflare.LogEvent
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter

  @subject Adaptor.DynatraceAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @url "https://172.32.0.1"
  @ingest_url "https://172.32.0.1/api/v2/logs/ingest"
  @valid_config %{url: @url, api_token: "dt0c01.test-token"}
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, type: :dynatrace, sources: [source], config: @valid_config)
    [backend: backend, source: source]
  end

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "config typecast and validation" do
    test "enforces required options" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})
      refute changeset.valid?
      assert errors_on(changeset).url == ["can't be blank"]
      assert errors_on(changeset).api_token == ["can't be blank"]
    end

    test "requires an https url" do
      for url <- ["abc12345.live.dynatrace.com", "http://172.32.0.1"] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{@valid_config_input | "url" => url})

        refute changeset.valid?
        assert "has invalid format" in errors_on(changeset).url
      end
    end

    test "rejects urls targeting private or reserved addresses" do
      for url <- ["https://127.0.0.1:9999/e/abc12345", "https://169.254.169.254/"] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{@valid_config_input | "url" => url})

        assert %Ecto.Changeset{valid?: false} = changeset, "expected SSRF block for #{url}"
        assert hd(errors_on(changeset).url) =~ "private or reserved IP addresses"
      end
    end

    test "accepts valid config" do
      changeset = Adaptor.cast_and_validate_config(@subject, @valid_config_input)
      assert changeset.valid?
    end
  end

  describe "client_opts/1" do
    test "authenticates with the Api-Token scheme against the ingest endpoint" do
      backend = build(:backend, type: :dynatrace, config: @valid_config)
      opts = @subject.client_opts(backend)

      assert opts[:url] == @ingest_url
      assert opts[:headers] == [{"authorization", "Api-Token dt0c01.test-token"}]
    end
  end

  describe "ingest_url/1" do
    test "appends the ingest path to SaaS and ActiveGate environment urls" do
      assert @subject.ingest_url("https://abc12345.live.dynatrace.com") ==
               "https://abc12345.live.dynatrace.com/api/v2/logs/ingest"

      assert @subject.ingest_url("https://activegate.example.com:9999/e/abc12345/") ==
               "https://activegate.example.com:9999/e/abc12345/api/v2/logs/ingest"
    end

    test "keeps a url that already points at the ingest endpoint" do
      assert @subject.ingest_url(@ingest_url) == @ingest_url
      assert @subject.ingest_url(@ingest_url <> "/") == @ingest_url
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succeeds when the empty batch is accepted", ctx do
      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == @ingest_url
        assert Tesla.get_header(env, "authorization") == "Api-Token dt0c01.test-token"
        assert Jason.decode!(env.body) == []

        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "succeeds when the empty batch is rejected as invalid input", ctx do
      mock_adapter(fn _env -> dynatrace_error(400, "Incorrect JSON format.") end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on a 400 not coming from the ingest API", ctx do
      mock_adapter(fn _env ->
        {:ok,
         %Tesla.Env{
           status: 400,
           body: "<html><body>Error: 400 Ambiguous URI empty segment</body></html>",
           headers: [{"content-type", "text/html;charset=iso-8859-1"}]
         }}
      end)

      assert {:error, :http_client_error} = @subject.test_connection(ctx.backend)
    end

    test "returns error on invalid token", ctx do
      for status <- [401, 403] do
        mock_adapter(fn _env -> dynatrace_error(status, "Token is missing required scope") end)

        assert {:error, :invalid_token} = @subject.test_connection(ctx.backend)
      end
    end

    test "returns error on client, server and transport failures", ctx do
      error_responses = [
        {dynatrace_error(404, "Not found"), :http_client_error},
        {dynatrace_error(402, "Licensing issue"), :http_client_error},
        {dynatrace_error(503, "Usable space limit reached"), :http_server_error},
        {{:error, :nxdomain}, :unknown_error}
      ]

      for {response, reason} <- error_responses do
        mock_adapter(fn _env -> response end)
        assert {:error, ^reason} = @subject.test_connection(ctx.backend)
      end
    end
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source} do
      stub(Logflare.Google.BigQuery, :get_table, fn _ -> {:error, :not_found} end)
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends log records via the ingest API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == @ingest_url
        assert Tesla.get_header(env, "authorization") == "Api-Token dt0c01.test-token"
        assert Tesla.get_header(env, "content-type") == "application/json"

        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      log_events = build_list(3, :log_event, source: source, message: "some msg")

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 5000

      records = Jason.decode!(body)
      assert length(records) == 3

      for record <- records do
        assert %{"content" => "some msg", "log.source" => source_name, "timestamp" => ts} =
                 record

        assert source_name == source.name
        assert {:ok, _, 0} = DateTime.from_iso8601(ts)
      end
    end
  end

  describe "log record formatter" do
    setup do
      [source: insert(:source, user: insert(:user))]
    end

    test "keeps the body as attributes", %{source: source} do
      log_event =
        build(:log_event,
          source: source,
          message: "some msg",
          level: "error",
          metadata: %{"user_id" => 1}
        )

      expected_timestamp =
        log_event.body["timestamp"]
        |> DateTime.from_unix!(:microsecond)
        |> DateTime.to_iso8601()

      assert [record] = format([log_event])

      assert %{
               "content" => "some msg",
               "event_message" => "some msg",
               "timestamp" => ^expected_timestamp,
               "level" => "error",
               "metadata" => %{"user_id" => 1},
               "log.source" => source_name
             } = record

      assert source_name == source.name
    end

    test "falls back to an empty content", %{source: source} do
      log_event =
        LogEvent.replace_body(build(:log_event, source: source), %{"timestamp" => 1_000_000})

      assert [%{"content" => "", "timestamp" => "1970-01-01T00:00:01.000000Z"}] =
               format([log_event])
    end

    test "empty array" do
      assert [] = format([])
    end
  end

  describe "redact_config/1" do
    test "redacts the access token" do
      assert %{api_token: "REDACTED", url: @url} = @subject.redact_config(@valid_config)
    end
  end

  describe "sanitize_config_for_display/1" do
    test "masks the access token and keeps the url" do
      assert %{url: @url, api_token: masked} = @subject.sanitize_config_for_display(@valid_config)
      refute masked == @valid_config.api_token
    end
  end

  defp format(log_events) do
    this = self()
    ref = make_ref()

    client =
      Tesla.client(
        [LogRecordFormatter],
        {MockAdapter,
         call: fn env ->
           send(this, {ref, env.body})
           {:ok, %Tesla.Env{status: 204, body: ""}}
         end}
      )

    {:ok, _env} = Tesla.post(client, "/", log_events)
    assert_receive {^ref, records}, 5000

    records
  end

  defp dynatrace_error(status, message) do
    {:ok,
     %Tesla.Env{
       status: status,
       body: %{"error" => %{"code" => status, "message" => message}},
       headers: [{"content-type", "application/json"}]
     }}
  end

  defp mock_adapter(function) do
    stub(@tesla_adapter)

    HttpBased.Client
    |> expect(:new, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> MockAdapter.replace(function)
    end)
  end
end
