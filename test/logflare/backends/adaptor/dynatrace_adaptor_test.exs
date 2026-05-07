defmodule Logflare.Backends.Adaptor.DynatraceAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.DynatraceAdaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject DynatraceAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "cast and validate" do
    test "url and api_token are required and url must be http(s)" do
      refute Adaptor.cast_and_validate_config(@subject, %{}).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "api_token" => "dt0c01.abc"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "https://abc.live.dynatrace.com"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "abc.live.dynatrace.com",
               "api_token" => "dt0c01.abc"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "https://abc.live.dynatrace.com",
               "api_token" => "dt0c01.abc"
             }).valid?
    end
  end

  describe "redact_config/1" do
    test "redacts api_token field" do
      config = %{url: "https://abc.live.dynatrace.com", api_token: "dt0c01.secret"}
      assert %{api_token: "REDACTED"} = @subject.redact_config(config)
    end
  end

  describe "test_connection/1" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :dynatrace,
          sources: [source],
          config: %{
            url: "https://abc.live.dynatrace.com",
            api_token: "dt0c01.token"
          }
        )

      [backend: backend]
    end

    test "POSTs an empty array to the env logs ingest path", %{backend: backend} do
      @client
      |> expect(:send, fn req ->
        assert req[:url] == "https://abc.live.dynatrace.com/api/v2/logs/ingest"
        assert req[:body] == []
        assert req[:headers]["Authorization"] == "Api-Token dt0c01.token"
        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "trailing slashes in url are normalized" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :dynatrace,
          sources: [source],
          config: %{
            url: "https://abc.live.dynatrace.com/",
            api_token: "dt0c01.token"
          }
        )

      @client
      |> expect(:send, fn req ->
        assert req[:url] == "https://abc.live.dynatrace.com/api/v2/logs/ingest"
        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns error on non-2xx response", %{backend: backend} do
      @client
      |> expect(:send, fn _req ->
        {:ok, %Tesla.Env{status: 401, body: %{"error" => %{"message" => "Unauthorized"}}}}
      end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "401"
    end

    test "returns error on transport failure", %{backend: backend} do
      @client
      |> expect(:send, fn _req -> {:error, :nxdomain} end)

      assert {:error, reason} = @subject.test_connection(backend)
      assert reason =~ "nxdomain"
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      source_with_service_name = insert(:source, user: user, service_name: "checkout-api")

      backend =
        insert(:backend,
          type: :dynatrace,
          sources: [source, source_with_service_name],
          config: %{
            url: "https://abc.live.dynatrace.com",
            api_token: "dt0c01.token"
          }
        )

      start_supervised!({AdaptorSupervisor, {source, backend}}, id: :source1)
      start_supervised!({AdaptorSupervisor, {source_with_service_name, backend}}, id: :source2)
      :timer.sleep(500)

      [
        backend: backend,
        source: source,
        source_with_service_name: source_with_service_name,
        user: user
      ]
    end

    test "sent logs are delivered", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn _req ->
        send(this, ref)
        %Tesla.Env{status: 204, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "nil event_message is handled correctly", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 204, body: ""}
      end)

      le = build(:log_event, source: source, message: nil, event_message: nil)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [entry]}, 2000
      assert entry["content"] == ""
    end

    test "service field is set to source.service_name", %{source_with_service_name: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 204, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [entry]}, 2000
      assert entry["service"] == source.service_name
    end

    test "service field falls back to source name", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 204, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [entry]}, 2000
      assert entry["service"] == source.name
    end

    test "timestamp is ISO 8601 and content is the event message", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 204, body: ""}
      end)

      le = build(:log_event, source: source, event_message: "hello world")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [entry]}, 2000
      assert entry["content"] == "hello world"
      assert {:ok, _, _} = DateTime.from_iso8601(entry["timestamp"])
      assert entry["log.source"] == "logflare"
      assert entry["data"] == le.body
    end
  end
end
