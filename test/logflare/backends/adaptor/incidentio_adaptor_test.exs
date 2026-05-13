defmodule Logflare.Backends.Adaptor.IncidentioAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Alerting

  @subject Logflare.Backends.Adaptor.IncidentioAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "cast and validate" do
    test "API token is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "alert_source_config_id" => "1234"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "api_token" => "1234"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "api_token" => "1234",
               "alert_source_config_id" => "1234"
             }).valid?

      # metadata
      assert Adaptor.cast_and_validate_config(@subject, %{
               "api_token" => "1234",
               "alert_source_config_id" => "1234",
               "metadata" => %{
                 "name" => "test",
                 "tags" => ["test"]
               }
             }).valid?
    end
  end

  describe "redact_config/1" do
    test "redacts api_token field" do
      config = %{api_token: "secret-api-token-123", alert_source_config_id: "config-123"}
      assert %{api_token: "REDACTED"} = @subject.redact_config(config)
    end
  end

  describe "events ingestion as alert events" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :incidentio,
          sources: [source],
          config: %{
            api_token: "1234",
            alert_source_config_id: "1234"
          }
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "sent alerts are delivered", %{source: source} do
      this = self()

      @client
      |> expect(:send, fn req ->
        send(this, req[:body])
        %Tesla.Env{status: 202, body: ""}
      end)

      %{body: %{"event_message" => message}} = le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive payload, 2000

      assert %{
               "title" => _,
               "status" => "firing",
               "description" => _,
               "metadata" => %{"data" => [%{"event_message" => ^message}]},
               "source_url" => alert_source_url
             } = payload

      # link to the backend
      assert alert_source_url =~ "/backends/"
    end
  end

  describe "alert query as alert events" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :incidentio,
          sources: [source],
          config: %{
            api_token: "1234",
            alert_source_config_id: "1234"
          }
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source, user: user]
    end

    test "alert queries that are run sends the results", %{user: user, backend: backend} do
      self = self()

      alert_query =
        insert(:alert,
          user: user,
          slack_hook_url: nil,
          webhook_notification_url: nil,
          backends: [backend],
          description: "test description",
          name: "test name"
        )

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn _conn, _proj_id, _opts ->
        {:ok, TestUtils.gen_bq_response([%{"testing" => "123"}])}
      end)

      @client
      |> expect(:send, fn req ->
        send(self, req[:body])
        %Tesla.Env{status: 202, body: ""}
      end)

      assert {:ok, _} = Alerting.run_alert(alert_query)

      assert_receive payload, 2000

      assert %{
               "title" => title,
               "status" => "firing",
               "description" => description,
               "metadata" => %{
                 "data" => [%{"testing" => "123"}]
               },
               "source_url" => alert_source_url
             } = payload

      # link to the alert
      assert alert_source_url =~ "/alerts/"
      assert title =~ alert_query.name
      assert description =~ alert_query.description
    end
  end
end
