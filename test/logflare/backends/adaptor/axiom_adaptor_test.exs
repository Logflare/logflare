defmodule Logflare.Backends.Adaptor.AxiomAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Adaptor.AxiomAdaptor
  @tesla_adapter Tesla.Adapter.Finch

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

  describe "test_connection/1" do
    setup :backend_data

    test "succceeds on 200 response", ctx do
      mock_adapter(fn env ->
        assert env.method == :post
        assert env.url == "https://api.axiom.co/v1/datasets/#{@valid_config.dataset_name}/ingest"
        assert Tesla.get_header(env, "authorization") == "Bearer #{@valid_config.api_token}"

        {:ok,
         %Tesla.Env{
           status: 200,
           body:
             ~s({"ingested":0,"failed":0,"failures":[],"processedBytes":2,"blocksCreated":0,"walLength":0})
         }}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on failure", ctx do
      error_responses = [
        {:ok,
         %Tesla.Env{status: 401, body: ~s({"code":401,"message":"forbidden"})}
         |> Tesla.put_header("content-type", "application/json")},
        {:ok,
         %Tesla.Env{
           status: 403,
           body: ~s({"code":403,"message":"not allowed to ingest into dataset"})
         }
         |> Tesla.put_header("content-type", "application/json")},
        {:error, :nxdomain}
      ]

      for response <- error_responses do
        mock_adapter(fn _env -> response end)

        assert {:error, reason} = @subject.test_connection(ctx.backend)
        assert is_binary(reason)
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
        # Based on https://axiom.co/docs/restapi/endpoints/ingestIntoDataset?playground=open
        assert Tesla.build_url(env) ==
                 "https://api.axiom.co/v1/datasets/#{@valid_config.dataset_name}/ingest?timestamp-field=timestamp&timestamp-format=2006-01-02T15%3A04%3A05.999999Z07%3A00"

        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/json"
        assert Tesla.get_header(env, "authorization") == "Bearer #{@valid_config.api_token}"
        assert Tesla.get_header(env, "content-encoding") == "gzip"

        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event =
        build(:log_event,
          source: source,
          event_message: "Test log message",
          random_attribute: "nothing",
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, gzipped}, 5000
      assert json = :zlib.gunzip(gzipped)
      assert [log] = Jason.decode!(json)
      assert log["event_message"] == log_event.body["event_message"]
      assert log["timestamp"]
      assert log["random_attribute"] == "nothing"
    end

    test "handles multiple log events in single batch", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_events =
        build_list(3, :log_event,
          source: source,
          timestamp: System.system_time(:microsecond)
        )

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, gzipped}, 5000
      assert json = :zlib.gunzip(gzipped)
      assert [_, _, _] = Jason.decode!(json)
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
