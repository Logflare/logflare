defmodule Logflare.Backends.Adaptor.OtlpAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.LogEvent
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsPartialSuccess
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @subject Adaptor.OtlpAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{
    endpoint: "http://localhost:4318/v1/logs",
    headers: %{},
    gzip: false,
    protocol: "http/protobuf"
  }
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :otlp,
        sources: [source],
        config: @valid_config
      )

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
      assert errors_on(changeset).endpoint == ["can't be blank"]
    end

    test "sets default options" do
      minimal_input = Map.take(@valid_config_input, ["endpoint"])

      changeset =
        Adaptor.cast_and_validate_config(@subject, minimal_input)

      assert changeset.valid?, inspect(changeset)

      assert %{gzip: true, protocol: "http/protobuf", headers: %{}, flatten_to_attributes: false} =
               Ecto.Changeset.apply_changes(changeset)
    end

    test "flatten_to_attributes defaults to false but can be enabled" do
      changeset =
        Adaptor.cast_and_validate_config(
          @subject,
          Map.put(@valid_config_input, "flatten_to_attributes", "true")
        )

      assert changeset.valid?
      assert %{flatten_to_attributes: true} = Ecto.Changeset.apply_changes(changeset)
    end

    test "allows to override the defaults" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "gzip" => "false",
            "headers" => %{"x-test" => "true"}
        })

      assert changeset.valid?

      assert %{gzip: false, headers: %{"x-test" => "true"}} =
               Ecto.Changeset.apply_changes(changeset)
    end

    test "downcases submitted header names" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "headers" => %{"Content-Type" => "application/json", "X-Custom" => "v"}
        })

      assert changeset.valid?

      assert %{headers: %{"content-type" => "application/json", "x-custom" => "v"}} =
               Ecto.Changeset.apply_changes(changeset)
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succceeds on 200 response", ctx do
      response_bodies =
        [
          %ExportLogsServiceResponse{partial_success: nil},
          %ExportLogsServiceResponse{partial_success: %ExportLogsPartialSuccess{}}
        ]
        |> Enum.map(&Protobuf.encode/1)

      for response_body <- response_bodies do
        mock_adapter(fn env ->
          assert env.method == :post
          assert env.url == "http://localhost:4318/v1/logs"

          {:ok,
           %Tesla.Env{
             status: 200,
             body: response_body,
             headers: [{"content-type", "application/x-protobuf"}]
           }}
        end)

        assert :ok = @subject.test_connection(ctx.backend)
      end
    end

    test "returns error on failure", ctx do
      error_responses = [
        {:ok, %Tesla.Env{status: 401, body: ""}},
        {:error, :nxdomain}
      ]

      for response <- error_responses do
        mock_adapter(fn _env -> response end)
        assert {:error, reason} = @subject.test_connection(ctx.backend)
        assert reason != nil
      end
    end

    test "resource falls back to describing Logflare, including service.version, when there's no source",
         ctx do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, IO.iodata_to_binary(env.body)})

        {:ok,
         %Tesla.Env{
           status: 200,
           body: Protobuf.encode(%ExportLogsServiceResponse{partial_success: nil}),
           headers: [{"content-type", "application/x-protobuf"}]
         }}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
      assert_receive {^ref, body}, 5000

      %{resource_logs: [%{resource: resource}]} = Protobuf.decode(body, ExportLogsServiceRequest)
      attrs = Map.new(resource.attributes, fn %{key: key, value: value} -> {key, value} end)

      assert any_value_to_term(attrs["service.name"]) == "Logflare"
      assert is_binary(any_value_to_term(attrs["service.version"]))
    end
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends logs via REST API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) == "http://localhost:4318/v1/logs"

        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/x-protobuf"

        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      msg = "Test log message"
      ts_us = DateTime.utc_now() |> DateTime.to_unix(:microsecond)

      log_event =
        build(:log_event,
          source: source,
          event_message: "Test log message",
          random_attribute: "nothing",
          timestamp: ts_us
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, body}, 5000
      assert request = Protobuf.decode(body, ExportLogsServiceRequest)
      assert %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} = request
      assert log_record.time_unix_nano == ts_us * 1000
      # legacy (default) shape: flatten_to_attributes isn't set on this backend,
      # so event_name still carries the message and everything else stays in body
      assert log_record.event_name == msg
      assert body =~ "random_attribute"
      assert body =~ "nothing"
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events =
        build_list(3, :log_event,
          source: source,
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 5000
      assert request = Protobuf.decode(body, ExportLogsServiceRequest)
      assert %{resource_logs: [%{scope_logs: [%{log_records: [_, _, _]}]}]} = request
    end

    test "hex-decodes trace_id and span_id into raw protobuf bytes", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      trace_id = "0102030405060708090a0b0c0d0e0f10"
      span_id = "0102030405060708"

      log_event =
        build(:log_event,
          source: source,
          trace_id: trace_id,
          span_id: span_id,
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, body}, 5000

      assert %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
               Protobuf.decode(body, ExportLogsServiceRequest)

      assert log_record.trace_id == Base.decode16!(trace_id, case: :mixed)
      assert byte_size(log_record.trace_id) == 16
      assert log_record.span_id == Base.decode16!(span_id, case: :mixed)
      assert byte_size(log_record.span_id) == 8
    end

    test "falls back to the raw value rather than raising when not valid hex", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event =
        build(:log_event,
          source: source,
          trace_id: "not-valid-hex",
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, body}, 5000

      assert %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
               Protobuf.decode(body, ExportLogsServiceRequest)

      assert log_record.trace_id == "not-valid-hex"
    end
  end

  describe "content-type header handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: %{@valid_config | headers: %{"Content-Type" => "application/x-protobuf"}}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(250)

      [source: source]
    end

    test "does not duplicate content-type header when one is already configured", %{
      source: source
    } do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.headers})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event = build(:log_event, source: source, timestamp: System.system_time(:microsecond))

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, headers}, 5000

      content_type_headers =
        Enum.filter(headers, fn {k, _v} -> String.downcase(k) == "content-type" end)

      assert content_type_headers == [{"content-type", "application/x-protobuf"}]
    end
  end

  describe "reserved headers" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: %{@valid_config | headers: %{"Content-Type" => "application/json"}}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(250)
      [source: source]
    end

    test "drops a user-supplied content-type so the formatter's is the only one", %{
      source: source
    } do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.headers})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event = build(:log_event, source: source, timestamp: System.system_time(:microsecond))

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, headers}, 5000

      content_types =
        for {key, value} <- headers, String.downcase(key) == "content-type", do: value

      assert content_types == ["application/x-protobuf"]
    end
  end

  describe "sanitize_config_for_display/1" do
    test "masks endpoint and headers while preserving displayable keys" do
      config = %{@valid_config | headers: %{"authorization" => "Bearer secret"}}

      assert %{
               endpoint: "**********",
               protocol: "http/protobuf",
               gzip: false,
               headers: "**********"
             } == @subject.sanitize_config_for_display(config)
    end

    test "does not leak credentials embedded in the endpoint" do
      config = %{
        @valid_config
        | endpoint: "https://user:secret123@collector.example.com/v1/logs?api-key=abc456"
      }

      sanitized = @subject.sanitize_config_for_display(config)

      assert sanitized.endpoint == "**********"
      refute inspect(sanitized) =~ "secret123"
      refute inspect(sanitized) =~ "abc456"
    end
  end

  describe "redact_config/1" do
    test "redacts sensitive headers" do
      config = %{
        @valid_config
        | headers: %{
            "authorization" => "Bearer secret",
            "x-api-key" => "secret",
            "x-auth-token" => "secret",
            "x-custom-header" => "not-a-secret"
          }
      }

      redacted_config = @subject.redact_config(config)

      assert redacted_config.headers["authorization"] == "REDACTED"
      assert redacted_config.headers["x-api-key"] == "REDACTED"
      assert redacted_config.headers["x-auth-token"] == "REDACTED"
      assert redacted_config.headers["x-custom-header"] == "not-a-secret"
    end
  end

  describe "encoding of real-world log samples (resource, scope, and severity)" do
    setup :backend_data

    setup %{source: source, backend: backend} do
      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(250)
      :ok
    end

    test "resource attributes describe the customer's source, not Logflare's own infra", %{
      source: source
    } do
      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{resource: resource}]} = Protobuf.decode(body, ExportLogsServiceRequest)

      attrs =
        Map.new(resource.attributes, fn %{key: key, value: value} ->
          {key, any_value_to_term(value)}
        end)

      assert attrs["service.name"] == (source.service_name || source.name)

      # no reliable version data exists for the customer's own service, so it's
      # omitted rather than reporting Logflare's version under the wrong name
      refute Map.has_key?(attrs, "service.version")

      refute Map.has_key?(attrs, "node")
      refute Map.has_key?(attrs, "cluster")
      refute Map.has_key?(attrs, "service")

      # no project_ref on this backend (not created via the log-drain flow), so
      # service.namespace is omitted rather than left blank
      refute Map.has_key?(attrs, "service.namespace")
    end

    test "resource service.name reflects source.service_name when set" do
      user = insert(:user)
      source = insert(:source, user: user, service_name: "my-distinctive-service")
      backend = insert(:backend, type: :otlp, sources: [source], config: @valid_config)

      start_supervised!({AdaptorSupervisor, {source, backend}}, id: :service_name_test_adaptor)
      :timer.sleep(250)

      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{resource: resource}]} = Protobuf.decode(body, ExportLogsServiceRequest)
      attrs = Map.new(resource.attributes, fn %{key: key, value: value} -> {key, value} end)

      assert any_value_to_term(attrs["service.name"]) == "my-distinctive-service"
    end

    test "resource service.namespace reflects the backend's project_ref when set" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: @valid_config,
          metadata: %{"project_ref" => "my-distinctive-project-ref", "type" => "log-drain"}
        )

      start_supervised!({AdaptorSupervisor, {source, backend}}, id: :namespace_test_adaptor)
      :timer.sleep(250)

      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{resource: resource}]} = Protobuf.decode(body, ExportLogsServiceRequest)
      attrs = Map.new(resource.attributes, fn %{key: key, value: value} -> {key, value} end)

      assert any_value_to_term(attrs["service.namespace"]) == "my-distinctive-project-ref"
    end

    test "scope identifies Logflare itself regardless of source", %{source: source} do
      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{scope: scope}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      assert scope.name == "Logflare"
      assert is_binary(scope.version) and scope.version != ""
    end

    for {fixture, severity_signal, expected_severity} <- [
          {"storage", ~s(metadata.level = "info"), :SEVERITY_NUMBER_INFO},
          {"realtime", ~s(metadata.level = "info"), :SEVERITY_NUMBER_INFO},
          {"edge_log", "metadata.response.status_code = 200", :SEVERITY_NUMBER_INFO},
          {"postgres", ~s(metadata.parsed.error_severity = "LOG"), :SEVERITY_NUMBER_INFO}
        ] do
      test "#{fixture}: severity_number is derived from #{severity_signal}",
           %{source: source} do
        log_event = load_fixture_log_event(unquote(fixture), source)
        body = capture_request_body(source, log_event)

        %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
          Protobuf.decode(body, ExportLogsServiceRequest)

        assert log_record.severity_number == unquote(expected_severity)
      end
    end

    test "severity_number maps HTTP status ranges to WARN/ERROR, not just INFO", %{
      source: source
    } do
      for {status, expected} <- [
            {200, :SEVERITY_NUMBER_INFO},
            {404, :SEVERITY_NUMBER_WARN},
            {500, :SEVERITY_NUMBER_ERROR}
          ] do
        log_event =
          "edge_log"
          |> load_fixture_log_event(source)
          |> put_in([Access.key!(:body), "metadata", "response", "status_code"], status)

        body = capture_request_body(source, log_event)

        %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
          Protobuf.decode(body, ExportLogsServiceRequest)

        assert log_record.severity_number == expected
      end
    end

    test "postgres error_severity takes priority over metadata.level when both are present", %{
      source: source
    } do
      log_event =
        "postgres"
        |> load_fixture_log_event(source)
        |> put_in([Access.key!(:body), "metadata", "level"], "debug")

      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      # metadata.parsed.error_severity is "LOG" -> INFO; if metadata.level ("debug")
      # had won instead this would be SEVERITY_NUMBER_DEBUG
      assert log_record.severity_number == :SEVERITY_NUMBER_INFO
    end

    test "postgres severity levels map through the full range", %{source: source} do
      for {pg_level, expected} <- [
            {"PANIC", :SEVERITY_NUMBER_FATAL},
            {"FATAL", :SEVERITY_NUMBER_FATAL},
            {"ERROR", :SEVERITY_NUMBER_ERROR},
            {"WARNING", :SEVERITY_NUMBER_WARN},
            {"NOTICE", :SEVERITY_NUMBER_INFO},
            {"LOG", :SEVERITY_NUMBER_INFO},
            {"DEBUG1", :SEVERITY_NUMBER_DEBUG}
          ] do
        log_event =
          "postgres"
          |> load_fixture_log_event(source)
          |> put_in([Access.key!(:body), "metadata", "parsed", "error_severity"], pg_level)

        body = capture_request_body(source, log_event)

        %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
          Protobuf.decode(body, ExportLogsServiceRequest)

        assert log_record.severity_number == expected
      end
    end
  end

  describe "flatten_to_attributes config option" do
    test "defaults to false and matches the legacy shape (everything in body, unflattened)" do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :otlp, sources: [source], config: @valid_config)

      start_supervised!({AdaptorSupervisor, {source, backend}}, id: :legacy_shape_test_adaptor)
      :timer.sleep(250)

      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      assert log_record.event_name == log_event.body["event_message"]
      assert log_record.attributes == []

      body_map = any_value_to_term(log_record.body)
      assert body_map["project"] == "test-project"
      assert get_in(body_map, ["metadata", "level"]) == "info"
    end

    test "when enabled, body is the plain event_message string and everything else is flattened into attributes" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: Map.put(@valid_config, :flatten_to_attributes, true)
        )

      start_supervised!({AdaptorSupervisor, {source, backend}},
        id: :structured_shape_test_adaptor
      )

      :timer.sleep(250)

      log_event = load_fixture_log_event("storage", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      assert log_record.event_name == ""
      assert log_record.body.value == {:string_value, log_event.body["event_message"]}

      attrs =
        Map.new(log_record.attributes, fn %{key: key, value: value} ->
          {key, any_value_to_term(value)}
        end)

      assert attrs["metadata.level"] == "info"
      refute Map.has_key?(attrs, "metadata")
      assert attrs["project"] == "test-project"
      refute Map.has_key?(attrs, "event_message")
    end

    test "when enabled, nested metadata fields become their own typed scalar attributes, not a stringified blob" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: Map.put(@valid_config, :flatten_to_attributes, true)
        )

      start_supervised!({AdaptorSupervisor, {source, backend}},
        id: :structured_flatten_test_adaptor
      )

      :timer.sleep(250)

      log_event = load_fixture_log_event("edge_log", source)
      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      attrs =
        Map.new(log_record.attributes, fn %{key: key, value: value} -> {key, value.value} end)

      # a real, typed int_value — not a string containing "200" and not buried
      # inside a JSON-stringified "metadata" blob
      assert attrs["metadata.response.status_code"] == {:int_value, 200}
      assert attrs["metadata.request.method"] == {:string_value, "GET"}
      assert attrs["metadata.request.cf.botManagement.score"] == {:int_value, 1}

      refute Map.has_key?(attrs, "metadata")
    end

    test "when enabled, merges an explicit attributes field with the source's other fields rather than one overwriting the other" do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :otlp,
          sources: [source],
          config: Map.put(@valid_config, :flatten_to_attributes, true)
        )

      start_supervised!({AdaptorSupervisor, {source, backend}},
        id: :structured_merge_test_adaptor
      )

      :timer.sleep(250)

      log_event =
        build(:log_event,
          source: source,
          attributes: %{"explicit_key" => "explicit_value", "nested" => %{"leaf" => "value"}},
          other_field: "other_value",
          timestamp: System.system_time(:microsecond)
        )

      body = capture_request_body(source, log_event)

      %{resource_logs: [%{scope_logs: [%{log_records: [log_record]}]}]} =
        Protobuf.decode(body, ExportLogsServiceRequest)

      attrs =
        Map.new(log_record.attributes, fn %{key: key, value: value} ->
          {key, any_value_to_term(value)}
        end)

      assert attrs["explicit_key"] == "explicit_value"
      assert attrs["other_field"] == "other_value"

      # explicit attributes are flattened the same way as the leftover fields
      assert attrs["nested.leaf"] == "value"
      refute Map.has_key?(attrs, "nested")
    end
  end

  @fixtures_dir Path.join([__DIR__, "..", "..", "..", "fixtures", "otlp_log_samples"])

  defp load_fixture_log_event(name, source) do
    @fixtures_dir
    |> Path.join("#{name}.json")
    |> File.read!()
    |> Jason.decode!()
    |> Map.put("timestamp", System.system_time(:microsecond))
    |> LogEvent.make(%{source: source})
  end

  defp any_value_to_term(%{value: {:string_value, v}}), do: v
  defp any_value_to_term(%{value: {:bool_value, v}}), do: v
  defp any_value_to_term(%{value: {:int_value, v}}), do: v
  defp any_value_to_term(%{value: {:double_value, v}}), do: v
  defp any_value_to_term(%{value: {:bytes_value, v}}), do: Base.encode64(v)

  defp any_value_to_term(%{value: {:array_value, %{values: values}}}),
    do: Enum.map(values, &any_value_to_term/1)

  defp any_value_to_term(%{value: {:kvlist_value, %{values: values}}}) do
    Map.new(values, fn %{key: k, value: v} -> {k, any_value_to_term(v)} end)
  end

  defp any_value_to_term(%{value: nil}), do: nil

  defp capture_request_body(source, log_event) do
    this = self()
    ref = make_ref()

    mock_adapter(fn env ->
      send(this, {ref, IO.iodata_to_binary(env.body)})
      {:ok, %Tesla.Env{status: 200, body: ""}}
    end)

    assert {:ok, _} = Backends.ingest_logs([log_event], source)
    assert_receive {^ref, body}, 5000
    body
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
