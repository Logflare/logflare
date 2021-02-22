defmodule Logflare.ChangefeedsTeset do
  @moduledoc false
  use Logflare.DataCase
  use Logflare.Commons
  import Logflare.Factory
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag :unboxed
  @moduletag :this

  def load_from_payload(schema, payload) do
    changes_from_payload =
      payload["changes"]
      |> MapKeys.to_atoms_unsafe!()
      |> User.changefeed_changeset()
      |> Map.get(:changes)

    Repo.load(schema, changes_from_payload)
  end

  def receive_payload(changefeed) do
    receive do
      {:notification, _pid, _ref, ^changefeed, payload} ->
        JSON.decode!(payload)

      msg ->
        flunk("Unexpected notification message: #{inspect(msg)}")
    after
      t = 1000 ->
        flunk("No message after #{t} ms")
    end
  end

  describe "Changefeeds" do
    @changefeed "users_changefeed"
    setup do
      Logflare.EctoSQLUnboxedHelpers.truncate_all()
      {:ok, pid} = Postgrex.Notifications.start_link(Repo.config())
      {:ok, ref} = Postgrex.Notifications.listen(pid, @changefeed)
      %{notify_pid: pid, ref: ref}
    end

    test "INSERT notifications", %{notify_pid: pid, ref: ref} do
      {:ok, db} = Users.insert_or_update_user(params_for(:user))
      %schema{} = db

      expected_payload = %{
        "table" => "users",
        "id" => db.id,
        "type" => "INSERT",
        "node_id" => "nonode@nohost",
        "changes" => Map.take(db, EctoSchemaReflection.fields(schema)) |> MapKeys.to_strings()
      }

      payload = receive_payload(@changefeed)

      assert db = load_from_payload(User, payload)
      assert Map.delete(expected_payload, "changes") == Map.delete(payload, "changes")
      mem = LocalRepo.get(User, db.id)
      assert db == mem
    end

    test "INSERT id only notifications", %{notify_pid: pid, ref: ref} do
      {:ok, ref} = Postgrex.Notifications.listen(pid, "source_schemas_id_only_changefeed")
      pid = Process.whereis(:source_schemas_id_only_changefeed_listener)

      Sandbox.allow(Repo, self(), pid)

      {:ok, u} = Users.insert_or_update_user(params_for(:user))
      {:ok, s} = Sources.create_source(params_for(:source), u)

      ss = SourceSchemas.get_source_schema_by(source_id: s.id)
      %schema{} = ss

      expected_payload = %{
        "table" => "users",
        "id" => u.id,
        "type" => "INSERT",
        "node_id" => "nonode@nohost",
        "changes" => Map.take(ss, EctoSchemaReflection.fields(schema)) |> MapKeys.to_strings()
      }

      payload = receive_payload(@changefeed)

      expected_payload = %{
        "table" => "source_schemas",
        "id" => ss.id,
        "node_id" => "nonode@nohost",
        "type" => "INSERT"
      }

      payload = receive_payload("source_schemas_id_only_changefeed")

      assert expected_payload == payload
    end

    test "UPDATE diffs only", %{notify_pid: pid, ref: ref} do
      {:ok, db} = Users.insert_or_update_user(params_for(:user))
      receive_payload(@changefeed)

      {:ok, db} = Users.update_user_allowed(db, %{name: "new name"})
      payload = receive_payload(@changefeed)

      assert payload == %{
               "changes" => %{
                 "name" => "new name"
               },
               "node_id" => "nonode@nohost",
               "id" => db.id,
               "table" => "users",
               "type" => "UPDATE"
             }

      Process.sleep(200)

      assert db == LocalRepo.get(User, db.id)
    end

    test "DELETE notifications", %{notify_pid: pid, ref: ref} do
      {:ok, db} = Users.insert_or_update_user(params_for(:user))
      Process.sleep(200)
      receive_payload(@changefeed)
      {:ok, user} = Users.delete_user(db)

      payload = receive_payload(@changefeed)

      assert payload == %{
               "id" => db.id,
               "table" => "users",
               "node_id" => "nonode@nohost",
               "type" => "DELETE",
               "changes" => nil
             }

      Process.sleep(300)
      assert is_nil(LocalRepo.get(User, db.id))
    end
  end
end
