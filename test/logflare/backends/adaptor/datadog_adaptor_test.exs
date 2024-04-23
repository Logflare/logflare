defmodule Logflare.Backends.Adaptor.DatadogAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.DatadogAdaptor

  doctest @subject

  describe "cast and validate" do
    test "API key is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      changeset =
        Adaptor.cast_and_validate_config(@subject, %{
          "api_key" => "foobarbaz"
        })

      assert changeset.valid?
    end
  end

  describe "logs ingestion" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)

      backend =
        insert(:backend, type: :datadog, sources: [source], config: %{api_key: "foo-bar"})

      pid = start_supervised!({@subject, {source, backend}})
      :timer.sleep(500)
      [pid: pid, backend: backend, source: source]
    end

    test "sent logs are delivered", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      Tesla.Mock.mock_global(fn _req ->
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

      Tesla.Mock.mock_global(fn req ->
        send(this, {ref, Jason.decode!(req.body)})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive {^ref, [log_entry]}, 2000
      assert log_entry["service"] == source.name
    end

    test "message is JSON encoded log event", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      Tesla.Mock.mock_global(fn req ->
        send(this, {ref, Jason.decode!(req.body)})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source)

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive {^ref, [log_entry]}, 2000
      assert Jason.decode!(log_entry["message"]) == le.body
    end
  end
end
