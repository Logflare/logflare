defmodule Logflare.Backends.Adaptor.SigNozAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @subject Adaptor.SigNozAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{
    endpoint: "https://ingest.us.signoz.cloud:443",
    ingestion_key: "test-ingestion-key"
  }
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  @keyless_config %{endpoint: "https://otel.example.com:4318"}

  defp backend_data(_ctx), do: insert_backend(@valid_config)

  defp keyless_backend_data(_ctx), do: insert_backend(@keyless_config)

  defp insert_backend(config) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :signoz,
        sources: [source],
        config: config
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

    test "validates the endpoint format" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "endpoint" => "ingest.us.signoz.cloud"
        })

      refute changeset.valid?
      assert errors_on(changeset).endpoint == ["has invalid format"]
    end

    test "rejects an endpoint that already includes the logs path" do
      for endpoint <- [
            "https://ingest.us.signoz.cloud:443/v1/logs",
            "https://ingest.us.signoz.cloud:443/v1/logs/"
          ] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{
            @valid_config_input
            | "endpoint" => endpoint
          })

        refute changeset.valid?

        assert errors_on(changeset).endpoint == [
                 "must not include the /v1/logs path, which is appended automatically"
               ]
      end
    end

    test "accepts valid config" do
      changeset = Adaptor.cast_and_validate_config(@subject, @valid_config_input)

      assert changeset.valid?
    end

    test "accepts config without an ingestion key" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "endpoint" => "https://otel.example.com:4318"
        })

      assert changeset.valid?
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succceeds on 200 response", ctx do
      response_body =
        %ExportLogsServiceResponse{partial_success: nil}
        |> Protobuf.encode()

      mock_adapter(fn env ->
        assert env.method == :post
        assert env.url == "https://ingest.us.signoz.cloud:443/v1/logs"
        assert Tesla.get_header(env, "signoz-ingestion-key") == "test-ingestion-key"

        {:ok,
         %Tesla.Env{
           status: 200,
           body: response_body,
           headers: [{"content-type", "application/x-protobuf"}]
         }}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on failure", ctx do
      error_responses = [
        {:ok, %Tesla.Env{status: 401, body: "unauthorized"}},
        {:error, :nxdomain}
      ]

      for response <- error_responses do
        mock_adapter(fn _env -> response end)
        assert {:error, _reason} = @subject.test_connection(ctx.backend)
      end
    end
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends logs via OTLP/HTTP", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) == "https://ingest.us.signoz.cloud:443/v1/logs"
        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/x-protobuf"
        assert Tesla.get_header(env, "signoz-ingestion-key") == "test-ingestion-key"

        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events = build_list(3, :log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 5000
      assert request = Protobuf.decode(body, ExportLogsServiceRequest)
      assert %{resource_logs: [%{scope_logs: [%{log_records: [_, _, _]}]}]} = request
    end
  end

  describe "log record shape" do
    setup :backend_data

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "puts the message in the body and flattens everything else into attributes", %{
      source: source
    } do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, IO.iodata_to_binary(env.body)})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event =
        build(:log_event,
          source: source,
          event_message: "duration: 3.456 ms  statement: SELECT 1",
          metadata: %{"parsed" => %{"error_severity" => "ERROR", "user_name" => "postgres"}}
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, body}, 5000

      assert %{resource_logs: [%{scope_logs: [%{log_records: [record]}]}]} =
               Protobuf.decode(body, ExportLogsServiceRequest)

      # SigNoz reads the body and attributes, and ignores event_name entirely.
      assert record.body.value ==
               {:string_value, "duration: 3.456 ms  statement: SELECT 1"}

      assert record.severity_number == :SEVERITY_NUMBER_ERROR

      attributes = Map.new(record.attributes, &{&1.key, &1.value.value})
      assert attributes["metadata.parsed.error_severity"] == {:string_value, "ERROR"}
      assert attributes["metadata.parsed.user_name"] == {:string_value, "postgres"}
    end
  end

  describe "logs ingestion without an ingestion key" do
    setup :keyless_backend_data

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "omits the ingestion key header", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) == "https://otel.example.com:4318/v1/logs"
        assert Tesla.get_header(env, "signoz-ingestion-key") == nil

        send(this, {ref, :sent})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events = build_list(1, :log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, :sent}, 5000
    end
  end

  describe "redact_config/1" do
    test "redacts the ingestion key" do
      assert @subject.redact_config(@valid_config).ingestion_key == "REDACTED"
    end

    test "leaves a config without an ingestion key untouched" do
      assert @subject.redact_config(@keyless_config) == @keyless_config
    end
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
