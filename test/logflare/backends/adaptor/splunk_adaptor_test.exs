defmodule Logflare.Backends.Adaptor.SplunkAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.SplunkAdaptor.HecFormatter
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter

  @subject Adaptor.SplunkAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  # A literal public IP keeps the SSRF middleware happy without a DNS lookup
  @url "https://172.32.0.1:8088/services/collector/event"
  @valid_config %{url: @url, token: "test-token"}
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, type: :splunk, sources: [source], config: @valid_config)
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
      assert errors_on(changeset).token == ["can't be blank"]
    end

    test "validates url format" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{@valid_config_input | "url" => "splunk:8088"})

      refute changeset.valid?
      assert "has invalid format" in errors_on(changeset).url
    end

    test "rejects urls targeting private or reserved addresses" do
      for url <- ["http://127.0.0.1/services/collector/event", "http://169.254.169.254/"] do
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

    test "accepts optional event fields" do
      params =
        Map.merge(@valid_config_input, %{
          "index" => "main",
          "source" => "logflare",
          "sourcetype" => "custom",
          "host" => "my-host"
        })

      changeset = Adaptor.cast_and_validate_config(@subject, params)
      assert changeset.valid?
    end
  end

  describe "client_opts/1" do
    test "authenticates with the HEC scheme" do
      backend = build(:backend, type: :splunk, config: @valid_config)
      opts = @subject.client_opts(backend)

      assert opts[:url] == @url
      assert opts[:headers] == [{"authorization", "Splunk test-token"}]
      assert opts[:json] == true
      assert opts[:gzip] == true
      assert opts[:ssrf] == true
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succeeds on the HEC `no data` response", ctx do
      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == @url
        assert Tesla.get_header(env, "authorization") == "Splunk test-token"

        hec_response(400, ~s({"text":"No data","code":5}))
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "succeeds on 200 response", ctx do
      mock_adapter(fn _env -> hec_response(200, ~s({"text":"Success","code":0})) end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on invalid token", ctx do
      mock_adapter(fn _env -> hec_response(403, ~s({"text":"Invalid token","code":4})) end)

      assert {:error, :invalid_token} = @subject.test_connection(ctx.backend)
    end

    test "returns error on incorrect index", ctx do
      mock_adapter(fn _env -> hec_response(400, ~s({"text":"Incorrect index","code":7})) end)

      assert {:error, :http_client_error} = @subject.test_connection(ctx.backend)
    end

    test "returns error on server and transport failures", ctx do
      error_responses = [
        {{:ok, %Tesla.Env{status: 503, body: "unavailable"}}, :http_server_error},
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
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends HEC events via the collector API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == @url
        assert Tesla.get_header(env, "authorization") == "Splunk test-token"
        assert Tesla.get_header(env, "content-type") == "application/json"
        assert Tesla.get_header(env, "content-encoding") == "gzip"

        send(this, {ref, :zlib.gunzip(env.body)})
        hec_response(200, ~s({"text":"Success","code":0}))
      end)

      log_events = build_list(3, :log_event, source: source, message: "some msg")

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 5000

      events = Jason.decode!(body)
      assert [%{"event" => %{"event_message" => "some msg"}}, _, _] = events

      for event <- events do
        assert %{"time" => time, "sourcetype" => "_json", "source" => source_name} = event
        assert source_name == source.name
        assert is_float(time) or is_integer(time)
        refute Map.has_key?(event, "index")
        refute Map.has_key?(event, "host")
      end
    end
  end

  describe "HEC envelope formatter" do
    setup do
      user = insert(:user)
      [source: insert(:source, user: user)]
    end

    test "with the configured event fields", %{source: source} do
      config = %{index: "main", source: "custom-source", sourcetype: "custom", host: "my-host"}
      log_event = build(:log_event, source: source, message: "some msg")
      timestamp = log_event.body["timestamp"]

      assert [event] = format([log_event], config)

      assert %{
               "event" => %{"event_message" => "some msg"},
               "time" => time,
               "index" => "main",
               "source" => "custom-source",
               "sourcetype" => "custom",
               "host" => "my-host"
             } = event

      assert time == timestamp / 1_000_000
    end

    test "empty array" do
      assert [] = format([], %{})
    end
  end

  describe "redact_config/1" do
    test "redacts the HEC token" do
      assert %{token: "REDACTED"} = @subject.redact_config(@valid_config)
    end
  end

  defp format(log_events, config) do
    this = self()
    ref = make_ref()

    client =
      Tesla.client(
        [{HecFormatter, config: config}],
        {MockAdapter,
         call: fn env ->
           send(this, {ref, env.body})
           {:ok, %Tesla.Env{status: 200, body: ""}}
         end}
      )

    {:ok, _env} = Tesla.post(client, "/", log_events)
    assert_receive {^ref, events}, 5000

    events
  end

  defp hec_response(status, body) do
    {:ok,
     %Tesla.Env{
       status: status,
       body: body,
       headers: [{"content-type", "application/json"}]
     }}
  end

  defp mock_adapter(calls_num \\ 1, function) do
    stub(@tesla_adapter)

    HttpBased.Client
    |> expect(:new, calls_num, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> MockAdapter.replace(function)
    end)
  end
end
