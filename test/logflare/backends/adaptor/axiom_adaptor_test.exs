defmodule Logflare.Backends.Adaptor.AxiomAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Adaptor.AxiomAdaptor

  @valid_config %{api_token: "THE-API-KEY", dataset_name: "logflare", domain: "api.axiom.co"}
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :axiom,
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
      assert errors_on(changeset).api_token == ["can't be blank"]
      assert errors_on(changeset).dataset_name == ["can't be blank"]
    end

    test "sets default options" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, Map.delete(@valid_config_input, "domain"))

      assert changeset.valid?
    end

    test "allows to override the defaults" do
      eu_domain = "api.eu.axiom.co"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          @valid_config_input
          | "domain" => eu_domain
        })

      assert changeset.valid?
      assert %{domain: ^eu_domain} = Ecto.Changeset.apply_changes(changeset)
    end
  end

  describe "transform_config/1" do
    test "converts config to WebhookAdaptor format" do
      backend = %{config: @valid_config}

      config = @subject.transform_config(backend)

      # Based on https://axiom.co/docs/restapi/endpoints/ingestIntoDataset?playground=open
      assert config.url ==
               "https://api.axiom.co/v1/datasets/#{@valid_config.dataset_name}/ingest?timestamp-field=timestamp&timestamp-format=2006-01-02T15%3A04%3A05.999999Z07%3A00"

      assert config.headers["content-type"] == "application/json"
      assert config.headers["authorization"] == "Bearer #{@valid_config.api_token}"
      assert config.http == "http2"
      assert config.gzip == true
      assert is_function(config.format_batch)
    end
  end

  describe "test_connection/1" do
    @tesla_adapter Tesla.Adapter.Finch

    setup :backend_data

    test "succceeds on 200 response", ctx do
      @tesla_adapter
      |> expect(:call, 2, fn env, _opts ->
        assert env.method == :get
        assert env.url == "https://api.axiom.co/v1/datasets/#{@valid_config.dataset_name}"
        assert Tesla.get_header(env, "authorization") == "Bearer #{@valid_config.api_token}"

        {:ok,
         %Tesla.Env{
           status: 200,
           body:
             """
             {
               "created": "2022-07-20T02:35:14.137Z",
               "description": "string",
               "id": "string",
               "kind": "axiom:events:v1",
               "name": "string",
               "who": "string"
             }
             """
             |> Jason.decode!()
         }}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
      assert :ok = @subject.test_connection({ctx.source, ctx.backend})
    end

    test "returns error on 403 response", ctx do
      @tesla_adapter
      |> expect(:call, fn _env, _opts ->
        {:ok,
         %Tesla.Env{
           status: 403,
           body:
             """
             {
               "code": 403,
               "message": "Forbidden"
             }
             """
             |> Jason.decode!()
         }}
      end)

      assert {:error, reason} = @subject.test_connection(ctx.backend)
      assert reason =~ "auth"
    end

    test "returns error on 404 response", ctx do
      @tesla_adapter
      |> expect(:call, fn _env, _opts ->
        {:ok,
         %Tesla.Env{
           status: 404,
           body:
             """
             {
               "code": 404,
               "message": "Not found"
             }
             """
             |> Jason.decode!()
         }}
      end)

      assert {:error, reason} = @subject.test_connection(ctx.backend)
      assert reason =~ @valid_config.dataset_name
    end

    test "returns error on 500 response", ctx do
      @tesla_adapter
      |> expect(:call, fn _env, _opts ->
        {:ok, %Tesla.Env{status: 500, body: ""}}
      end)

      assert {:error, reason} = @subject.test_connection(ctx.backend)
      assert is_binary(reason)
    end

    test "returns error on request error", ctx do
      @tesla_adapter
      |> expect(:call, fn _env, _opts ->
        {:error, :nxdomain}
      end)

      assert {:error, :nxdomain} = @subject.test_connection(ctx.backend)
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

      Client
      |> expect(:send, fn opts ->
        body = opts[:body]

        assert opts[:headers]["content-type"] == "application/json"
        assert opts[:headers]["authorization"] == "Bearer #{@valid_config.api_token}"

        send(this, {ref, body})
        %Tesla.Env{status: 200, body: ""}
      end)

      log_event =
        build(:log_event,
          source: source,
          event_message: "Test log message",
          random_attribute: "nothing",
          timestamp: 1_704_067_200_000_000
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, [log]}, 5000
      assert log["event_message"] == log_event.body["event_message"]
      assert log["timestamp"] == "2024-01-01T00:00:00.000000Z"
      assert log["random_attribute"] == "nothing"
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      Client
      |> expect(:send, fn req ->
        body = req[:body]
        send(this, {ref, body})
        %Tesla.Env{status: 200, body: ""}
      end)

      log_events =
        build_list(3, :log_event,
          source: source,
          timestamp: 1_704_067_200_000_000
        )

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, [_, _, _]}, 5000
    end
  end

  describe "redact_config/1" do
    test "redacts API Token" do
      token = "AN_API_TOKEN"
      config = %{api_token: token, domain: "api.axiom.co", dataset_name: "logflare"}

      redacted_token = @subject.redact_config(config).api_token
      refute redacted_token == token
      assert redacted_token == "REDACTED"
    end
  end
end
