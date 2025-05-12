defmodule Logflare.Backends.Adaptor.ClickhouseAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.SystemMetrics.AllLogsLogged

  @subject Logflare.Backends.Adaptor.ClickhouseAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  setup do
    start_supervised!(AllLogsLogged)
    :ok
  end

  describe "cast and validate" do
    test "Username and Password are required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://localhost:1234",
               "database" => "default",
               "table" => "supabase_log_ingress",
               "port" => 8443,
               "username" => "foo",
               "password" => "bar"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "foobarbaz",
               "database" => "default",
               "table" => "supabase_log_ingress",
               "port" => 8443,
               "username" => "foo",
               "password" => "bar"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://localhost:1234",
               "database" => "default",
               "table" => "supabase_log_ingress",
               "port" => nil,
               "username" => "foo",
               "password" => "bar"
             }).valid?
    end
  end

  describe "logs ingestion" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :clickhouse,
          sources: [source],
          config: %{
            url: "http://localhost:1234",
            database: "default",
            table: "supabase_log_ingress",
            port: 8443,
            username: "foo",
            password: "bar"
          }
        )

      start_supervised!({AdaptorSupervisor, {source, backend}})
      :timer.sleep(500)
      [backend: backend, source: source]
    end

    test "payload is a properly formatted log event", %{source: source} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert {:ok, _} = Backends.ingest_logs([le], source)
      assert_receive {^ref, [log_entry]}, 2000

      assert is_map(log_entry)
      assert log_entry["id"] == le.id
      assert log_entry["event_message"] == "test-msg"
      assert log_entry["timestamp"] == le.body["timestamp"]

      assert is_map(log_entry["body"])
    end
  end
end
