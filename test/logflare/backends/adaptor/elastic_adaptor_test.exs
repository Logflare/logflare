defmodule Logflare.Backends.Adaptor.ElasticAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.LogEvent

  @subject Logflare.Backends.Adaptor.ElasticAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "cast and validate" do
    test "filebeat requires url; username/password must both be set when used" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "filebeat",
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz",
               "password" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz"
             }).valid?
    end

    test "defaults transport to filebeat when omitted" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "url" => "http://foobarbaz.com"
        })

      assert changeset.valid?
      assert Ecto.Changeset.apply_changes(changeset).transport == "filebeat"
    end

    test "otlp transport requires endpoint" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "otlp"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "otlp",
               "endpoint" =>
                 "https://abc.ingest.us-central1.gcp.elastic.cloud:443/supabase/v1/logs"
             }).valid?
    end

    test "rejects unknown transport" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "beats",
               "url" => "http://foobarbaz.com"
             }).valid?
    end

    test "logstash transport requires a valid http url" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "logstash"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "logstash",
               "url" => "logstash.internal:8080"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "logstash",
               "url" => "http://logstash.internal:8080"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "logstash",
               "url" => "http://logstash.internal:8080",
               "username" => "foobarbaz"
             }).valid?
    end
  end

  describe "sanitize_config_for_display/1" do
    test "masks credentials while preserving url" do
      config = %{username: "user", password: "secret123", url: "https://example.com"}

      assert %{username: "**********", password: "**********", url: "https://example.com"} ==
               @subject.sanitize_config_for_display(config)
    end
  end

  describe "redact_config/1" do
    test "redacts password field when present" do
      config = %{password: "secret123", url: "https://example.com"}
      assert %{password: "REDACTED"} = @subject.redact_config(config)
    end

    test "leaves config unchanged when password is not present" do
      config = %{url: "https://example.com"}
      assert ^config = @subject.redact_config(config)
    end

    test "redacts sensitive otlp headers" do
      config = %{
        transport: "otlp",
        endpoint: "https://example.com/v1/logs",
        headers: %{
          "Authorization" => "secret",
          "x-custom-header" => "ok"
        }
      }

      redacted = @subject.redact_config(config)
      assert redacted.headers["Authorization"] == "REDACTED"
      assert redacted.headers["x-custom-header"] == "ok"
    end
  end

  describe "only url" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :elastic,
          sources: [source],
          config: %{url: "http://localhost:1234"}
        )

      start_supervised!({SourceSup, source})
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

    test "sends events as-is", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [event]}, 2000
      assert event["some"] == "key"
    end
  end

  describe "basic auth" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :elastic,
          sources: [source],
          config: %{
            url: "http://localhost:1234",
            username: "some user",
            password: "some password"
          }
        )

      pid = start_supervised!({SourceSup, source})
      [pid: pid, backend: backend, source: source]
    end

    test "adds authorization header", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        assert "Basic" <> _ = req[:headers]["Authorization"]
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [event]}, 2000
      assert event["some"] == "key"
    end
  end

  describe "format_batch/1" do
    test "lifts timestamp and event_message into ECS fields" do
      log_event = %LogEvent{
        id: "0286d5cf-0e6a-4f4a-9ae0-3a0f8b6c0001",
        body: %{
          "timestamp" => 1_757_239_200_000_000,
          "event_message" => "hello logstash",
          "my_field" => "abc"
        },
        source_name: "my-source",
        event_type: :log
      }

      assert [event] = @subject.format_batch([log_event])
      assert event["@timestamp"] == "2025-09-07T10:00:00.000000Z"
      assert event["message"] == "hello logstash"
      assert event["my_field"] == "abc"

      assert event["logflare"] == %{
               "id" => "0286d5cf-0e6a-4f4a-9ae0-3a0f8b6c0001",
               "source" => "my-source",
               "source_uuid" => nil,
               "event_type" => "log"
             }

      refute Map.has_key?(event, "timestamp")
      refute Map.has_key?(event, "event_message")
    end

    test "falls back to ingest time when the event has no timestamp" do
      log_event = %LogEvent{
        body: %{"event_message" => "no timestamp"},
        ingested_at: ~U[2025-09-07 10:00:00.000000Z]
      }

      assert [%{"@timestamp" => "2025-09-07T10:00:00.000000Z"}] =
               @subject.format_batch([log_event])
    end

    test "omits @timestamp and message when neither is available" do
      assert [event] = @subject.format_batch([%LogEvent{body: %{"my_field" => "abc"}}])

      refute Map.has_key?(event, "@timestamp")
      refute Map.has_key?(event, "message")
      assert event["my_field"] == "abc"
    end
  end

  describe "logstash transport" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :elastic,
          sources: [source],
          config: %{
            transport: "logstash",
            url: "http://localhost:8080",
            username: "some user",
            password: "some password"
          }
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "delivers ECS-shaped events with basic auth", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        assert "Basic" <> _ = req[:headers]["Authorization"]
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, event_message: "hello logstash", some: "key")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [event]}, 2000

      assert {:ok, %DateTime{}, _} = DateTime.from_iso8601(event["@timestamp"])
      assert event["message"] == "hello logstash"
      assert event["some"] == "key"
      assert event["logflare"]["source"] == source.name
      refute Map.has_key?(event, "timestamp")
      refute Map.has_key?(event, "event_message")
    end

    test "test_connection/1 probes the input with an empty batch", %{backend: backend} do
      @client
      |> expect(:send, fn req ->
        assert req[:url] == "http://localhost:8080"
        assert req[:body] == []
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      assert :ok = @subject.test_connection(backend)
    end
  end
end
