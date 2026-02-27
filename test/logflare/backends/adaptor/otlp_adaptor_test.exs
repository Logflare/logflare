defmodule Logflare.Backends.Adaptor.OtlpAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
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

      assert %{gzip: true, protocol: "http/protobuf", headers: %{}} =
               Ecto.Changeset.apply_changes(changeset)
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
  end

  describe "logs ingestion" do
    setup :backend_data

    setup %{source: source, backend: backend} do
      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(250)
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

  defp mock_adapter(calls_num \\ 1, function) do
    stub(@tesla_adapter)

    HttpBased.Client
    |> expect(:new, calls_num, fn opts ->
      HttpBased.Client
      |> Mimic.call_original(:new, [opts])
      |> Logflare.Tesla.MockAdapter.replace(function)
    end)
  end
end
