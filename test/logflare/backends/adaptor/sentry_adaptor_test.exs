defmodule Logflare.Backends.Adaptor.SentryAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends.Adaptor.SentryAdaptor
  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  @subject SentryAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    :ok
  end

  describe "cast and validate" do
    test "DSN is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?
      assert %{dsn: ["can't be blank"]} = errors_on(changeset)
    end

    test "valid DSN passes validation" do
      valid_dsn = "https://abc123@o123456.ingest.sentry.io/123456"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => valid_dsn
        })

      assert changeset.valid?
    end

    test "DSN with secret key passes validation" do
      valid_dsn = "https://public:secret@o123456.ingest.sentry.io/123456"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => valid_dsn
        })

      assert changeset.valid?
    end

    test "invalid DSN format fails validation" do
      invalid_dsn = "not-a-valid-dsn"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end

    test "DSN without project ID fails validation" do
      invalid_dsn = "https://abc123@sentry.io/not-a-number"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end

    test "DSN with query parameters fails validation" do
      invalid_dsn = "https://abc123@sentry.io/123456?param=value"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "dsn" => invalid_dsn
        })

      refute changeset.valid?
      assert %{dsn: [error]} = errors_on(changeset)
      assert error =~ "Invalid DSN"
    end
  end

  describe "DSN parsing" do
    test "parse/1 successfully parses valid DSN" do
      dsn = "https://abc123@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.original_dsn == dsn
      assert parsed.public_key == "abc123"
      assert parsed.secret_key == nil
      assert parsed.endpoint_uri == "https://o123456.ingest.sentry.io/api/123456/envelope/"
    end

    test "parse/1 handles DSN with secret key" do
      dsn = "https://public:secret@o123456.ingest.sentry.io/123456"

      assert {:ok, parsed} = DSN.parse(dsn)
      assert parsed.public_key == "public"
      assert parsed.secret_key == "secret"
    end

    test "parse/1 fails with invalid format" do
      assert {:error, _reason} = DSN.parse("invalid-dsn")
    end

    test "parse/1 fails with query parameters" do
      dsn = "https://abc123@sentry.io/123456?param=value"
      assert {:error, reason} = DSN.parse(dsn)
      assert reason =~ "query parameters"
    end
  end

  describe "transform_config/1" do
    test "converts DSN to webhook configuration" do
      backend = %{
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      config = @subject.transform_config(backend)

      assert config.url == "https://o123456.ingest.sentry.io/api/123456/envelope/"
      assert config.headers == %{"content-type" => "application/x-sentry-envelope"}
      assert config.http == "http2"
      assert is_function(config.format_batch)
    end

    test "raises error with invalid DSN" do
      backend = %{
        config: %{dsn: "invalid-dsn"}
      }

      assert_raise ArgumentError, ~r/Invalid Sentry DSN/, fn ->
        @subject.transform_config(backend)
      end
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

    # Helper function to normalize envelope for snapshot comparison
    defp normalize_envelope_for_snapshot(envelope_body) do
      lines = String.split(envelope_body, "\n")
      [header_line, item_header_line, item_payload_line] = lines

      # Parse and normalize header (replace dynamic timestamp)
      header = Jason.decode!(header_line)
      normalized_header = Map.put(header, "sent_at", "2024-01-01T00:00:00.000000Z")

      # Parse and normalize item payload (replace dynamic log timestamps and trace_ids)
      item_payload = Jason.decode!(item_payload_line)
      normalized_items =
        Enum.map(item_payload["items"], fn item ->
          item
          |> Map.put("timestamp", 1.7040672e9) # Keep scientific notation format
          |> Map.delete("trace_id") # Remove dynamic trace_id for consistent snapshots
        end)
      normalized_item_payload = Map.put(item_payload, "items", normalized_items)

      # Reconstruct envelope with normalized data
      [
        Jason.encode!(normalized_header),
        item_header_line,
        Jason.encode!(normalized_item_payload)
      ]
      |> Enum.join("\n")
    end

    test "sent logs are delivered as Sentry envelope", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]

        send(this, {ref, envelope_body})
        %Tesla.Env{status: 200, body: ""}
      end)

      # Create log event with fixed timestamp for consistent snapshot
      le = build(:log_event,
        source: source,
        body: %{
          "timestamp" => 1704067200_000_000, # 2024-01-01T00:00:00Z in microseconds
          "message" => "Test log message"
        }
      )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, envelope_body}, 2000

      # Normalize envelope and compare against snapshot
      normalized_envelope = normalize_envelope_for_snapshot(envelope_body)

      expected_envelope = """
      {"dsn":"https://abc123@o123456.ingest.sentry.io/123456","sent_at":"2024-01-01T00:00:00.000000Z"}
      {"content_type":"application/vnd.sentry.items.log+json","item_count":1,"type":"log"}
      {"items":[{"attributes":{"logflare.source.id":{"type":"integer","value":#{source.id}},"logflare.source.name":{"type":"string","value":"#{source.name}"},"sentry.sdk.name":{"type":"string","value":"sentry.logflare"},"sentry.sdk.version":{"type":"string","value":"0.1.0"}},"body":"test-msg","level":"info","timestamp":1.7040672e9}]}\
      """

      assert normalized_envelope == expected_envelope
    end

    test "log events are properly transformed to Sentry format", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]

        send(this, {ref, envelope_body})
        %Tesla.Env{status: 200, body: ""}
      end)

      le =
        build(:log_event,
          source: source,
          message: "Test log message",
          body: %{
            "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
            "message" => "Test log message",
            "level" => "error",
            "user_id" => 123,
            "metadata" => %{"request_id" => "abc-123"}
          }
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, envelope_body}, 2000

      # Normalize envelope and compare against snapshot
      normalized_envelope = normalize_envelope_for_snapshot(envelope_body)

      expected_envelope = """
      {"dsn":"https://abc123@o123456.ingest.sentry.io/123456","sent_at":"2024-01-01T00:00:00.000000Z"}
      {"content_type":"application/vnd.sentry.items.log+json","item_count":1,"type":"log"}
      {"items":[{"attributes":{"logflare.source.id":{"type":"integer","value":#{source.id}},"logflare.source.name":{"type":"string","value":"#{source.name}"},"metadata":{"type":"string","value":"%{\\\"request_id\\\" => \\\"abc-123\\\"}"},"sentry.sdk.name":{"type":"string","value":"sentry.logflare"},"sentry.sdk.version":{"type":"string","value":"0.1.0"},"user_id":{"type":"integer","value":123}},"body":"Test log message","level":"error","timestamp":1.7040672e9}]}\
      """

      assert normalized_envelope == expected_envelope
    end

    test "handles log events without explicit level", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]

        send(this, {ref, envelope_body})
        %Tesla.Env{status: 200, body: ""}
      end)

      le =
        build(:log_event,
          source: source,
          body: %{
            "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
            "message" => "Test message without level"
          }
        )

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, envelope_body}, 2000

      # Normalize envelope and compare against snapshot
      normalized_envelope = normalize_envelope_for_snapshot(envelope_body)

      expected_envelope = """
      {"dsn":"https://abc123@o123456.ingest.sentry.io/123456","sent_at":"2024-01-01T00:00:00.000000Z"}
      {"content_type":"application/vnd.sentry.items.log+json","item_count":1,"type":"log"}
      {"items":[{"attributes":{"logflare.source.id":{"type":"integer","value":#{source.id}},"logflare.source.name":{"type":"string","value":"#{source.name}"},"sentry.sdk.name":{"type":"string","value":"sentry.logflare"},"sentry.sdk.version":{"type":"string","value":"0.1.0"}},"body":"test-msg","level":"info","timestamp":1.7040672e9}]}\
      """

      assert normalized_envelope == expected_envelope
    end

    test "properly maps different log levels", %{source: source} do
      level_mappings = [
        {"debug", "debug"},
        {"info", "info"},
        {"notice", "info"},
        {"warning", "warn"},
        {"warn", "warn"},
        {"error", "error"},
        {"critical", "fatal"},
        {"alert", "fatal"},
        {"emergency", "fatal"},
        {"unknown", "info"}
      ]

      for {input_level, expected_level} <- level_mappings do
        this = self()
        ref = make_ref()

        le =
          build(:log_event,
            source: source,
            body: %{
              "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
              "message" => "Test message",
              "level" => input_level
            }
          )

        @client
        |> expect(:send, fn req ->
          envelope_body = req[:body]
          send(this, {ref, envelope_body})
          %Tesla.Env{status: 200, body: ""}
        end)

        assert {:ok, _} = Backends.ingest_logs([le], source)
        assert_receive {^ref, envelope_body}, 2000

        # Normalize envelope and verify level mapping via snapshot
        normalized_envelope = normalize_envelope_for_snapshot(envelope_body)

        # Build expected envelope dynamically to ensure proper interpolation
        expected_envelope = [
          "{\"dsn\":\"https://abc123@o123456.ingest.sentry.io/123456\",\"sent_at\":\"2024-01-01T00:00:00.000000Z\"}",
          "{\"content_type\":\"application/vnd.sentry.items.log+json\",\"item_count\":1,\"type\":\"log\"}",
          "{\"items\":[{\"attributes\":{\"logflare.source.id\":{\"type\":\"integer\",\"value\":#{source.id}},\"logflare.source.name\":{\"type\":\"string\",\"value\":\"#{source.name}\"},\"sentry.sdk.name\":{\"type\":\"string\",\"value\":\"sentry.logflare\"},\"sentry.sdk.version\":{\"type\":\"string\",\"value\":\"0.1.0\"}},\"body\":\"test-msg\",\"level\":\"#{expected_level}\",\"timestamp\":1.7040672e9}]}"
        ] |> Enum.join("\n")

        assert normalized_envelope == expected_envelope,
               "Expected #{input_level} to map to #{expected_level} in envelope"
      end
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        envelope_body = req[:body]
        send(this, {ref, envelope_body})
        %Tesla.Env{status: 200, body: ""}
      end)

      log_events = [
        build(:log_event,
          source: source,
          message: "Log 1",
          body: %{
            "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
            "message" => "Log 1"
          }
        ),
        build(:log_event,
          source: source,
          message: "Log 2",
          body: %{
            "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
            "message" => "Log 2"
          }
        ),
        build(:log_event,
          source: source,
          message: "Log 3",
          body: %{
            "timestamp" => 1704067200_000_000, # Fixed timestamp for snapshot
            "message" => "Log 3"
          }
        )
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, envelope_body}, 2000

      # Normalize envelope and compare against snapshot
      normalized_envelope = normalize_envelope_for_snapshot(envelope_body)

      # Parse the normalized envelope to verify structure and content
      lines = String.split(normalized_envelope, "\n")
      [header_line, item_header_line, item_payload_line] = lines

      # Check header
      header = Jason.decode!(header_line)
      assert header["dsn"] == "https://abc123@o123456.ingest.sentry.io/123456"
      assert header["sent_at"] == "2024-01-01T00:00:00.000000Z"

      # Check item header
      item_header = Jason.decode!(item_header_line)
      assert item_header["content_type"] == "application/vnd.sentry.items.log+json"
      assert item_header["item_count"] == 3
      assert item_header["type"] == "log"

      # Check items (order-agnostic since log processing can vary order)
      item_payload = Jason.decode!(item_payload_line)
      items = item_payload["items"]
      assert length(items) == 3

      # Check that all expected messages are present
      messages = Enum.map(items, fn item -> item["body"] end)
      assert "Log 1" in messages
      assert "Log 2" in messages
      assert "Log 3" in messages

      # Check that all items have the expected normalized structure
      for item <- items do
        assert item["level"] == "info"
        assert item["timestamp"] == 1.7040672e9 # Normalized timestamp
        assert is_nil(item["trace_id"]) # trace_id removed for normalization

        attributes = item["attributes"]
        assert attributes["logflare.source.id"]["value"] == source.id
        assert attributes["logflare.source.name"]["value"] == source.name
        assert attributes["sentry.sdk.name"]["value"] == "sentry.logflare"
        assert attributes["sentry.sdk.version"]["value"] == "0.1.0"
      end
    end

    test "handles empty log events list without crashing", %{source: _source} do
      backend = %Logflare.Backends.Backend{
        type: :sentry,
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      transformed_config = @subject.transform_config(backend)

      # Call format_batch with empty list - should not crash
      result = transformed_config.format_batch.([])

      # Normalize and compare against snapshot
      normalized_envelope = normalize_envelope_for_snapshot(result)

      expected_envelope = """
      {"dsn":"https://abc123@o123456.ingest.sentry.io/123456","sent_at":"2024-01-01T00:00:00.000000Z"}
      {"content_type":"application/vnd.sentry.items.log+json","item_count":0,"type":"log"}
      {"items":[]}\
      """

      assert normalized_envelope == expected_envelope
    end
  end

  describe "trace ID handling" do
    test "extract_trace_id generates consistent ID for invalid trace_id", %{} do
      backend = %Logflare.Backends.Backend{
        type: :sentry,
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      transformed_config = @subject.transform_config(backend)

      test_cases = [
        {
          "invalid short trace_id",
          %{
            "trace_id" => "invalid-too-short",
            "message" => "Test message",
            "timestamp" => 1704067200_000_000
          }
        },
        {
          "all zeros trace_id",
          %{
            "trace_id" => "00000000000000000000000000000000",
            "message" => "Test message",
            "timestamp" => 1704067200_000_000
          }
        },
        {
          "no trace_id present",
          %{
            "message" => "Test message",
            "timestamp" => 1704067200_000_000
          }
        }
      ]

      for {description, body_data} <- test_cases do
        le = build(:log_event, body: body_data)
        result = transformed_config.format_batch.([le])

        # Parse the envelope to check trace_id format
        lines = String.split(result, "\n")
        item_payload = Jason.decode!(Enum.at(lines, 2))
        item = Enum.at(item_payload["items"], 0)

        # Should be a valid 32-character hex string
        assert String.length(item["trace_id"]) == 32, "Failed for case: #{description}"
        assert String.match?(item["trace_id"], ~r/^[0-9a-f]+$/), "Failed for case: #{description}"

        # Should not be the invalid input values
        case description do
          "invalid short trace_id" -> assert item["trace_id"] != "invalid-too-short"
          "all zeros trace_id" -> assert item["trace_id"] != "00000000000000000000000000000000"
          _ -> :ok
        end
      end
    end
  end

  describe "attribute building and data type conversion" do
    test "handles different data types in attributes", %{} do
      le = build(:log_event, body: %{
        "timestamp" => 1704067200_000_000,
        "message" => "Test message",
        "string_field" => "text_value",
        "integer_field" => 42,
        "float_field" => 3.14,
        "boolean_field" => true,
        "null_like_field" => "null",
        "list_field" => [1, 2, 3],
        "map_field" => %{"nested" => "value"}
      })

      backend = %Logflare.Backends.Backend{
        type: :sentry,
        config: %{dsn: "https://abc123@o123456.ingest.sentry.io/123456"}
      }

      transformed_config = @subject.transform_config(backend)
      result = transformed_config.format_batch.([le])

      # Parse the envelope to inspect data type conversion
      lines = String.split(result, "\n")
      item_payload = Jason.decode!(Enum.at(lines, 2))
      item = Enum.at(item_payload["items"], 0)
      attributes = item["attributes"]

      # Check data type conversions
      assert attributes["string_field"] == %{"type" => "string", "value" => "text_value"}
      assert attributes["integer_field"] == %{"type" => "integer", "value" => 42}
      assert attributes["float_field"] == %{"type" => "double", "value" => 3.14}
      assert attributes["boolean_field"] == %{"type" => "boolean", "value" => true}
      assert attributes["null_like_field"] == %{"type" => "string", "value" => "null"}
      assert attributes["list_field"] == %{"type" => "string", "value" => "[1, 2, 3]"}
      assert attributes["map_field"] == %{"type" => "string", "value" => "%{\"nested\" => \"value\"}"}
    end
  end

  describe "execute_query/3" do
    test "returns not implemented error" do
      result = @subject.execute_query(nil, "SELECT 1", [])
      assert {:error, :not_implemented} = result
    end
  end
end
