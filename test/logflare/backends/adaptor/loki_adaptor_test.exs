defmodule Logflare.Backends.Adaptor.LokiAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.LokiAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "test_connection/1" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :loki,
          sources: [source],
          config: %{url: "https://loki.example.com/loki/api/v1/push"}
        )

      [backend: backend]
    end

    test "POSTs an empty `streams` body to the configured URL", %{backend: backend} do
      @client
      |> expect(:send, fn req ->
        assert req[:url] == "https://loki.example.com/loki/api/v1/push"
        assert req[:body] == %{streams: []}
        {:ok, %Tesla.Env{status: 204, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end

    test "returns error on non-2xx response", %{backend: backend} do
      @client
      |> expect(:send, fn _req ->
        {:ok, %Tesla.Env{status: 401, body: "no auth"}}
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

  describe "cast and validate" do
    test "API key is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://localhost:1234"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://localhost:1234",
               "headers" => %{
                 "Authorization" => "1234"
               }
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz",
               "password" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://localhost:1234",
               "headers" => "foobarbaz"
             }).valid?
    end
  end

  describe "redact_config/1" do
    test "redacts password field when present" do
      config = %{password: "secret123", url: "https://loki.example.com"}
      assert %{password: "REDACTED"} = @subject.redact_config(config)
    end

    test "leaves config unchanged when password is not present" do
      config = %{url: "https://loki.example.com"}
      assert ^config = @subject.redact_config(config)
    end
  end

  describe "logs ingestion" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :loki,
          sources: [source],
          config: %{url: "http://localhost:1234"}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "sent logs are delivered", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn _req ->
        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "stream label is set", %{source: %{name: source_name} = source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, log_entry}, 2000

      assert %{streams: [%{stream: %{source: "supabase", service: ^source_name}}]} = log_entry
    end

    test "message is JSON encoded log event", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, payload}, 2000

      assert %{
               streams: [
                 %{
                   values: [
                     [ts, message, %{}]
                   ]
                 }
               ]
             } = payload

      assert ts =~ inspect(le.body["timestamp"])

      assert %{"event_message" => ^message} = le.body
    end
  end
end
