defmodule Logflare.Backends.Adaptor.ElasticAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.SourceSup
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.ElasticAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    insert(:plan)
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "cast and validate" do
    test "filebeat requires url; username/password must both be set when used" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "filebeat",
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz",
               "password" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz"
             }).valid?
    end

    test "defaults transport to filebeat when omitted" do
      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "url" => "http://foobarbaz.com"
        })

      assert changeset.valid?
      assert Ecto.Changeset.apply_changes(changeset).transport == "filebeat"
    end

    test "otlp transport requires endpoint" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "otlp"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "otlp",
               "endpoint" => "https://abc.ingest.us-central1.gcp.elastic.cloud:443/supabase/v1/logs"
             }).valid?
    end

    test "rejects unknown transport" do
      refute Adaptor.cast_and_validate_config(@subject, %{
               "transport" => "logstash",
               "url" => "http://foobarbaz.com"
             }).valid?
    end
  end

  describe "redact_config/1" do
    test "redacts password field when present" do
      config = %{password: "secret123", url: "https://example.com"}
      assert %{password: "REDACTED"} = @subject.redact_config(config)
    end

    test "leaves config unchanged when password is not present" do
      config = %{url: "https://example.com"}
      assert ^config = @subject.redact_config(config)
    end

    test "redacts sensitive otlp headers" do
      config = %{
        transport: "otlp",
        endpoint: "https://example.com/v1/logs",
        headers: %{
          "Authorization" => "secret",
          "x-custom-header" => "ok"
        }
      }

      redacted = @subject.redact_config(config)
      assert redacted.headers["Authorization"] == "REDACTED"
      assert redacted.headers["x-custom-header"] == "ok"
    end
  end

  describe "only url" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :elastic,
          sources: [source],
          config: %{url: "http://localhost:1234"}
        )

      start_supervised!({SourceSup, source})
      [backend: backend, source: source]
    end

    test "sent logs are delivered", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn _req ->
        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive ^ref, 2000
    end

    test "sends events as-is", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [event]}, 2000
      assert event["some"] == "key"
    end
  end

  describe "basic auth" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :elastic,
          sources: [source],
          config: %{
            url: "http://localhost:1234",
            username: "some user",
            password: "some password"
          }
        )

      pid = start_supervised!({SourceSup, source})
      [pid: pid, backend: backend, source: source]
    end

    test "adds authorization header", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        assert "Basic" <> _ = req[:headers]["Authorization"]
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [event]}, 2000
      assert event["some"] == "key"
    end
  end
end
