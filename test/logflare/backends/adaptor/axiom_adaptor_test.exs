defmodule Logflare.Backends.Adaptor.AxiomAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Adaptor.AxiomAdaptor

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
      api_token = "THE-API-KEY"
      dataset_name = "logflare"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "api_token" => api_token,
          "dataset_name" => dataset_name
        })

      assert changeset.valid?

      data = Ecto.Changeset.apply_changes(changeset)
      assert map_size(data) == 3
      assert data.api_token == api_token
      assert data.dataset_name == dataset_name
      assert data.domain == "api.axiom.co"
    end

    test "allows to override the defaults" do
      api_token = "THE-API-KEY"
      dataset_name = "logflare"
      eu_domain = "api.eu.axiom.co"

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "api_token" => api_token,
          "dataset_name" => dataset_name,
          "domain" => eu_domain
        })

      assert changeset.valid?

      data = Ecto.Changeset.apply_changes(changeset)
      assert map_size(data) == 3
      assert data.api_token == api_token
      assert data.dataset_name == dataset_name
      assert data.domain == eu_domain
    end
  end

  describe "transform_config/1" do
    test "converts config to WebhookAdaptor format" do
      api_token = "THE-API-KEY"
      dataset_name = "logflare"
      domain = "api.axiom.co"

      backend = %{
        config: %{api_token: api_token, dataset_name: dataset_name, domain: domain}
      }

      config = @subject.transform_config(backend)

      # Based on https://axiom.co/docs/restapi/endpoints/ingestIntoDataset?playground=open
      assert config.url ==
               "https://api.axiom.co/v1/datasets/#{dataset_name}/ingest?timestamp-field=timestamp&timestamp-format=2006-01-02T15%3A04%3A05.999999Z07%3A00"

      assert config.headers["content-type"] == "application/json"
      assert config.headers["authorization"] == "Bearer #{api_token}"
      assert config.http == "http2"
      assert config.gzip == true
      assert is_function(config.format_batch)
    end
  end

  describe "connection check" do
    # TODO
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      api_token = "THE-API-KEY"
      dataset_name = "logflare"
      domain = "api.axiom.co"

      config = %{api_token: api_token, dataset_name: dataset_name, domain: domain}

      backend =
        insert(:backend,
          type: :axiom,
          sources: [source],
          config: config
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source, config: config]
    end

    test "sends logs via REST API", %{source: source, config: config} do
      this = self()
      ref = make_ref()

      Client
      |> expect(:send, fn opts ->
        body = opts[:body]

        assert opts[:headers]["content-type"] == "application/json"
        assert opts[:headers]["authorization"] == "Bearer #{config.api_token}"

        send(this, {ref, body})
        %Tesla.Env{status: 200, body: ""}
      end)

      message = "Test log message"

      log_events = [
        build(:log_event,
          source: source,
          event_message: message,
          random_attribute: "nothing",
          timestamp: 1_704_067_200_000_000
        )
      ]

      assert {:ok, _} = Backends.ingest_logs(log_events, source)
      assert_receive {^ref, body}, 2000
      assert [log] = body
      assert log["event_message"] == message
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
      assert_receive {^ref, body}, 2000

      assert length(body) == 3

      # Check that all expected messages are present
      messages = Enum.map(body, fn item -> item["event_message"] end)
      assert "Log 1" in messages
      assert "Log 2" in messages
      assert "Log 3" in messages
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
