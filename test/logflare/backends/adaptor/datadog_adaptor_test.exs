defmodule Logflare.Backends.Adaptor.DatadogAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.DatadogAdaptor
  @client Logflare.Backends.Adaptor.WebhookAdaptor.Client

  doctest @subject

  describe "cast and validate" do
    test "API key is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "api_key" => "foobarbaz",
               "region" => "US1"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "api_key" => "foobarbaz"
             }).valid?
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend,
          type: :datadog,
          sources: [source],
          config: %{api_key: "foo-bar", region: "US1"}
        )

      pid = start_supervised!({@subject, {source, backend}})
      :timer.sleep(500)
      [pid: pid, backend: backend, source: source]
    end

    test "sent logs are delivered", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn _req ->
        send(this, ref)
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive ^ref, 2000
    end

    test "service field is set to source name", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry.service == source.name
    end

    test "message is JSON encoded log event", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      @client
      |> expect(:send, fn req ->
        send(this, {ref, req[:body]})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry.message =~ Jason.encode!(le.body)
    end
  end
end
