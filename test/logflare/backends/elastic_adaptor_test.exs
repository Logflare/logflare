defmodule Logflare.Backends.Adaptor.ElasticAdaptorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor

  @subject Logflare.Backends.Adaptor.ElasticAdaptor

  doctest @subject

  describe "cast and validate" do
    test "API key is required" do
      changeset = Adaptor.cast_and_validate_config(@subject, %{})

      refute changeset.valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com"
             }).valid?

      assert Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz",
               "password" => "foobarbaz"
             }).valid?

      refute Adaptor.cast_and_validate_config(@subject, %{
               "url" => "http://foobarbaz.com",
               "username" => "foobarbaz"
             }).valid?
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

    test "sends events as-is", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      Tesla.Mock.mock_global(fn req ->
        send(this, {ref, Jason.decode!(req.body)})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
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

      pid = start_supervised!({@subject, {source, backend}})
      :timer.sleep(500)
      [pid: pid, backend: backend, source: source]
    end

    test "adds authorization header", %{pid: pid, source: source, backend: backend} do
      this = self()
      ref = make_ref()

      Tesla.Mock.mock_global(fn req ->
        assert {_k, "Basic" <> _} = List.keyfind(req.headers, "Authorization")
        send(this, {ref, Jason.decode!(req.body)})
        %Tesla.Env{status: 200, body: ""}
      end)

      le = build(:log_event, source: source, some: "key")

      assert :ok == @subject.ingest(pid, [le], source_id: source.id, backend_id: backend.id)
      assert_receive {^ref, [event]}, 2000
      assert event["some"] == "key"
    end
  end
end
