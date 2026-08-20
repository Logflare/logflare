defmodule Logflare.Backends.Adaptor.GoogleSecOpsAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Tesla.MockAdapter

  @subject Adaptor.GoogleSecOpsAdaptor
  @tesla_adapter Tesla.Adapter.Finch

  @valid_config %{
    region: "europe-west3",
    project_number: "123456789",
    instance_id: "3b7a442e-0f57-4d13-a1ce-9f21bd7ea083",
    feed_id: "b2a4b2c1-9df3-4c4d-8a24-2d8f9d7be091",
    api_key: "THE-API-KEY",
    secret: "THE-SECRET"
  }
  @valid_config_input Map.new(@valid_config, fn {k, v} -> {Atom.to_string(k), v} end)

  @expected_url "https://europe-west3-chronicle.googleapis.com/v1" <>
                  "/projects/123456789/locations/europe-west3" <>
                  "/instances/3b7a442e-0f57-4d13-a1ce-9f21bd7ea083" <>
                  "/feeds/b2a4b2c1-9df3-4c4d-8a24-2d8f9d7be091:importPushLogs"

  defp backend_data(_ctx) do
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :google_secops,
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

      for field <- [:region, :project_number, :instance_id, :feed_id, :api_key, :secret] do
        assert errors_on(changeset)[field] == ["can't be blank"]
      end
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

    test "validates identifier formats" do
      for {field, value} <- [
            {"project_number", "123/evil"},
            {"instance_id", "not-a-uuid"},
            {"instance_id", "x/../../evil"},
            {"feed_id", "b2a4b2c1-9df3-4c4d-8a24-2d8f9d7be091:evil"}
          ] do
        changeset =
          Adaptor.cast_and_validate_config(@subject, %{@valid_config_input | field => value})

        refute changeset.valid?
        assert errors_on(changeset)[String.to_existing_atom(field)] == ["has invalid format"]
      end
    end

    test "accepts valid config" do
      changeset = Adaptor.cast_and_validate_config(@subject, @valid_config_input)
      assert changeset.valid?
    end
  end

  describe "test_connection/1" do
    setup :backend_data

    test "succeeds on 200 response", ctx do
      mock_adapter(fn env ->
        assert env.method == :post
        assert env.url == @expected_url
        assert Tesla.get_header(env, "x-goog-api-key") == @valid_config.api_key
        assert Tesla.get_header(env, "x-webhook-access-key") == @valid_config.secret

        {:ok, %Tesla.Env{status: 200, body: "{}"}}
      end)

      assert :ok = @subject.test_connection(ctx.backend)
    end

    test "returns error on failure", ctx do
      error_responses = [
        {:ok, %Tesla.Env{status: 401, body: ~s({"error":{"message":"unauthorized"}})}},
        {:ok, %Tesla.Env{status: 404, body: ~s({"error":{"message":"feed not found"}})}},
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

    setup %{source: source} do
      start_supervised!({SourceSup, source})
      :ok
    end

    test "sends newline-delimited events in a single request", %{source: source} do
      this = self()
      ref = make_ref()

      mock_adapter(fn env ->
        assert env.method == :post
        assert Tesla.build_url(env) == @expected_url
        assert Tesla.get_header(env, "content-type") == "application/json"
        assert Tesla.get_header(env, "x-goog-api-key") == @valid_config.api_key
        assert Tesla.get_header(env, "x-webhook-access-key") == @valid_config.secret

        send(this, {ref, env.body})
        {:ok, %Tesla.Env{status: 200, body: "{}"}}
      end)

      log_events =
        for message <- ["one", "two", "three"] do
          build(:log_event,
            source: source,
            event_message: message,
            timestamp: System.system_time(:microsecond)
          )
        end

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 5000

      assert [_, _, _] = lines = String.split(IO.iodata_to_binary(body), "\n")

      messages =
        for line <- lines do
          assert %{"event_message" => message, "timestamp" => _} = Jason.decode!(line)
          message
        end

      assert Enum.sort(messages) == ["one", "three", "two"]
    end
  end

  describe "redact_config/1" do
    test "redacts API key and secret" do
      redacted = @subject.redact_config(@valid_config)

      assert redacted.api_key == "REDACTED"
      assert redacted.secret == "REDACTED"
      assert redacted.region == @valid_config.region
    end

    test "does not materialize absent keys" do
      assert @subject.redact_config(%{secret: "s"}) == %{secret: "REDACTED"}
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
