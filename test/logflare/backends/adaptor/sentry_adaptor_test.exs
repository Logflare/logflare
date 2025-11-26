defmodule Logflare.Backends.Adaptor.SentryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.SentryAdaptor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject SentryAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "cast and validate" do
    test "DSN validation" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})
      refute changeset.valid?
      assert %{dsn: ["can't be blank"]} = errors_on(changeset)

      valid_dsn = "https://abc123@o123456.ingest.sentry.io/123456"
      changeset = Adaptor.cast_and_validate_config(@subject, %{"dsn" => valid_dsn})
      assert changeset.valid?, "Expected #{valid_dsn} to be valid"
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :sentry,
          sources: [source],
          config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "sends logs as a serialized sentry envelope", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == "https://o123456.ingest.sentry.io/api/123456/envelope/"
        assert Tesla.get_header(env, "content-type") == "application/x-sentry-envelope"
        envelope_body = env.body

        send(this, {ref, envelope_body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events = [
        build(:log_event,
          source: source,
          event_message: "Test log message",
          timestamp: 1_704_067_200_000_000
        )
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, envelope_body}, 2000

      [header_line, item_header_line, item_payload_line] = String.split(envelope_body, "\n")

      header = Jason.decode!(header_line)
      assert header["dsn"] == "https://abc123@o123456.ingest.sentry.io/123456"
      assert header["sent_at"]

      item_header = Jason.decode!(item_header_line)
      assert item_header["content_type"] == "application/vnd.sentry.items.log+json"
      assert item_header["item_count"] == 1
      assert item_header["type"] == "log"

      item_payload = Jason.decode!(item_payload_line)
      items = item_payload["items"]
      assert length(items) == 1
      item = Enum.at(items, 0)

      assert item["attributes"]["logflare.source.name"] == %{
               "type" => "string",
               "value" => source.name
             }

      assert item["attributes"]["logflare.source.uuid"] == %{
               "type" => "string",
               "value" => inspect(source.token)
             }

      assert item["attributes"]["sentry.sdk.name"] == %{
               "type" => "string",
               "value" => "sentry.logflare"
             }

      assert item["attributes"]["sentry.sdk.version"]
      assert item["body"] == "Test log message"
      assert item["level"] == "info"
      assert item["timestamp"]
    end

    test "maps to different log levels", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      level_mappings = [
        {"debug", "debug"},
        {"info", "info"},
        {"notice", "info"},
        {"warning", "warn"},
        {"warn", "warn"},
        {"error", "error"},
        {"fatal", "fatal"},
        {"critical", "fatal"},
        {"alert", "fatal"},
        {"emergency", "fatal"},
        {"unknown", "info"}
      ]

      log_events =
        Enum.with_index(level_mappings)
        |> Enum.map(fn {{input_level, _expected_level}, index} ->
          build(:log_event,
            source: source,
            event_message: "Test log message",
            timestamp: 1_704_067_200_000_000,
            level: input_level,
            test_index: index
          )
        end)

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, envelope_body}, 2000

      [_header_line, _item_header_line, item_payload_line] = String.split(envelope_body, "\n")
      item_payload = Jason.decode!(item_payload_line)
      items = item_payload["items"]
      assert length(items) == length(level_mappings)

      # Check each item's level mapping using the test_index attribute
      for item <- items do
        test_index = item["attributes"]["test_index"]["value"]
        {_input_level, expected_level} = Enum.at(level_mappings, test_index)

        assert item["level"] == expected_level,
               "Item with test_index #{test_index} has level #{item["level"]}, expected #{expected_level}"
      end
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events = [
        build(:log_event,
          source: source,
          event_message: "Log 1",
          timestamp: 1_704_067_200_000_000
        ),
        build(:log_event,
          source: source,
          event_message: "Log 2",
          timestamp: 1_704_067_200_000_000
        ),
        build(:log_event,
          source: source,
          event_message: "Log 3",
          timestamp: 1_704_067_200_000_000
        )
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, envelope_body}, 2000

      [_header_line, item_header_line, item_payload_line] = String.split(envelope_body, "\n")

      item_header = Jason.decode!(item_header_line)
      assert item_header["item_count"] == 3

      item_payload = Jason.decode!(item_payload_line)
      items = item_payload["items"]
      assert length(items) == 3

      # Check that all expected messages are present
      messages = Enum.map(items, fn item -> item["body"] end)
      assert "Log 1" in messages
      assert "Log 2" in messages
      assert "Log 3" in messages
    end

    test "handles different data types in attributes", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events = [
        build(:log_event,
          source: source,
          event_message: "Test message",
          trace_id: "efdb9350effb47959d48bd0aaf395824",
          timestamp: 1_704_067_200_000_000,
          string_field: "text_value",
          integer_field: 42,
          float_field: 3.14,
          boolean_field: true,
          list_field: [1, 2, 3],
          metadata: %{
            "project" => "testing_123",
            "level" => "info",
            "region" => "us-west-1",
            "context" => %{
              "application" => "realtime",
              "module" => "Elixir.Realtime.Telemetry.Logger"
            }
          }
        )
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, envelope_body}, 2000

      [_header_line, _item_header_line, item_payload_line] = String.split(envelope_body, "\n")
      item_payload = Jason.decode!(item_payload_line)
      items = item_payload["items"]
      assert length(items) == 1
      item = Enum.at(items, 0)

      assert item["trace_id"] == "efdb9350effb47959d48bd0aaf395824"

      attributes = item["attributes"]
      assert attributes["string_field"] == %{"type" => "string", "value" => "text_value"}
      assert attributes["integer_field"] == %{"type" => "integer", "value" => 42}
      assert attributes["float_field"] == %{"type" => "double", "value" => 3.14}
      assert attributes["boolean_field"] == %{"type" => "boolean", "value" => true}
      assert attributes["list_field"] == %{"type" => "string", "value" => "[1,2,3]"}

      assert attributes["metadata"] == %{
               "type" => "string",
               "value" =>
                 "{\"context\":{\"application\":\"realtime\",\"module\":\"Elixir.Realtime.Telemetry.Logger\"},\"level\":\"info\",\"project\":\"testing_123\",\"region\":\"us-west-1\"}"
             }
    end
  end

  describe "redact_config/1" do
    test "redacts DSN secret key" do
      config = %{dsn: "https://public_key:secret_key@o123456.ingest.sentry.io/123456"}

      assert %{dsn: "https://public_key:REDACTED@o123456.ingest.sentry.io/123456"} =
               @subject.redact_config(config)

      # no secret key
      config = %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}

      assert %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"} =
               @subject.redact_config(config)
    end
  end

  defp mock_adapter(function) do
    stub(@tesla_adapter)

    HttpBased.Client
    |> expect(:new, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> Logflare.Tesla.MockAdapter.replace(function)
    end)
  end
end
