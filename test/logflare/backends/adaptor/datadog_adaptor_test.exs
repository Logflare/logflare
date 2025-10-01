defmodule Logflare.Backends.Adaptor.DatadogAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends.Adaptor.DatadogAdaptor

  @subject DatadogAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "cast and validate" do
    test "API key is required and region only accepts valid regions" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "api_key" => "foobarbaz",
               "region" => "US1"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "api_key" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "api_key" => "foobarbaz",
               "region" => "US0"
             }).valid?
    end
  end

  describe "redact_config/1" do
    test "redacts api_key field" do
      config = %{api_key: "secret-api-key-123", region: "US1"}
      assert %{api_key: "REDACTED"} = @subject.redact_config(config)
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :datadog,
          sources: [source],
          config: %{api_key: "foo-bar", region: "US1"}
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

    test "bug: nil event_message is still handled correctly ", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn _req ->
        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, message: nil, event_message: nil)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "service field is set to source.service_name", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: %{source | service_name: "some name"})

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry["service"] == "some name"
    end

    test "service field falls back to source name", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry["service"] == source.name
    end

    test "message contains log event body", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry["data"] == le.body
    end
  end
end
