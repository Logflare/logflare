defmodule Logflare.Backends.Adaptor.Last9AdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @subject Adaptor.Last9Adaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{
    region: "US-WEST-1",
    username: "testuser",
    password: "testpassword"
  }
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :last9,
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
      assert errors_on(changeset).region == ["can't be blank"]
      assert errors_on(changeset).username == ["can't be blank"]
      assert errors_on(changeset).password == ["can't be blank"]
    end

    test "validates region" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "region" => "invalid-region"
        })

      refute changeset.valid?
      assert errors_on(changeset).region == ["is invalid"]
    end

    test "accepts valid config" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, @valid_config_input)

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
        assert env.url == "https://otlp.last9.io/v1/logs"
        assert Tesla.get_header(env, "authorization") == "Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk"

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
        {:ok, %Tesla.Env{status: 401, body: "forbidden"}},
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

    setup %{source: source, backend: backend} do
      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(250)
      :ok
    end

    test "sends logs via REST API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) == "https://otlp.last9.io/v1/logs"
        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/x-protobuf"
        assert Tesla.get_header(env, "authorization") == "Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk"

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

  describe "redact_config/1" do
    test "redacts username and password" do
      redacted_config = @subject.redact_config(@valid_config)
      assert redacted_config.password == "REDACTED"
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
