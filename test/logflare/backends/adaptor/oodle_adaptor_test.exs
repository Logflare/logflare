defmodule Logflare.Backends.Adaptor.OodleAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Adaptor.OodleAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{
    instance: "inst-fujifilm-awb12",
    api_key: "7edc7d5e-3aa8-4328-9735-74c0f97b921d"
  }

  @valid_config_input Map.new(@valid_config, fn {k, v} ->
                        {Atom.to_string(k), v}
                      end)

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :oodle,
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
      assert errors_on(changeset).instance == ["can't be blank"]
      assert errors_on(changeset).api_key == ["can't be blank"]
    end

    test "accepts required options" do
      changeset =
        Adaptor.cast_and_validate_config(
          @subject,
          @valid_config_input
        )

      assert changeset.valid?
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succeeds on 200 response", ctx do
      mock_adapter(fn env ->
        assert env.method == :post

        assert env.url ==
                 "https://#{@valid_config.instance}-logs." <>
                   "collector.oodle.ai/ingest/v1/logs"

        assert Tesla.get_header(env, "X-OODLE-INSTANCE") ==
                 @valid_config.instance

        assert Tesla.get_header(env, "X-API-KEY") == @valid_config.api_key

        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on failure", ctx do
      error_responses = [
        {:ok,
         %Tesla.Env{
           status: 403,
           body: ~s({"message":"forbidden"})
         }
         |> Tesla.put_header("content-type", "application/json")},
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

    test "sends logs via HTTP API", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert Tesla.build_url(env) ==
                 "https://#{@valid_config.instance}-logs." <>
                   "collector.oodle.ai/ingest/v1/logs"

        assert env.method == :post
        assert Tesla.get_header(env, "content-type") == "application/json"
        assert Tesla.get_header(env, "content-encoding") == "gzip"
        assert Tesla.get_header(env, "X-OODLE-INSTANCE") ==
                 @valid_config.instance

        assert Tesla.get_header(env, "X-API-KEY") == @valid_config.api_key

        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: ""}}
      end)

      log_event = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([log_event], source)
      assert_receive {^ref, gzipped}, 5000
      assert json = :zlib.gunzip(gzipped)
      assert [log] = Jason.decode!(json)
      assert log["event_message"] == log_event.body["event_message"]
    end
  end

  describe "redact_config/1" do
    test "redacts API key" do
      redacted_config = @subject.redact_config(@valid_config)
      assert redacted_config.api_key == "REDACTED"
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
