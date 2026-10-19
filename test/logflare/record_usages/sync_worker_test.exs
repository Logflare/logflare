defmodule Logflare.RecordUsages.SyncWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Auth
  alias Logflare.Cluster.Utils
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.RecordUsage
  alias Logflare.RecordUsages.SyncWorker
  alias Logflare.User
  alias Logflare.Users

  for {count, offsets} <- [
        {1, [0]},
        {7, [0, 42_857, 85_714, 128_571, 171_428, 214_285, 257_142]},
        {100, Enum.to_list(0..297_000//3_000)}
      ] do
    test "parent durably schedules #{count} sorted nodes across five minutes" do
      nodes = for index <- 1..unquote(count), do: :"usage-#{index}@localhost"
      stub(Utils, :node_list_all, fn -> Enum.reverse(nodes) end)
      reject(&RecordUsages.flush/0)
      before = DateTime.utc_now()

      assert :ok = perform_job(SyncWorker, %{})

      after_scheduling = DateTime.utc_now()
      jobs = Repo.all(from(job in Oban.Job, order_by: job.scheduled_at))
      assert length(jobs) == unquote(count)
      [first | _] = jobs
      assert DateTime.compare(first.scheduled_at, before) in [:eq, :gt]
      assert DateTime.compare(first.scheduled_at, after_scheduling) in [:eq, :lt]

      assert Enum.map(jobs, & &1.args) ==
               nodes |> Enum.sort() |> Enum.map(&%{"node" => Atom.to_string(&1)})

      assert Enum.map(jobs, & &1.scheduled_at) ==
               Enum.map(unquote(offsets), &DateTime.add(first.scheduled_at, &1, :millisecond))

      assert Enum.all?(jobs, &(&1.queue == "default" and &1.max_attempts == 1))
      assert Enum.all?(jobs, &(&1.state == "scheduled"))
    end
  end

  test "parent insertion suppresses duplicates for 60 seconds, including after completion" do
    stub(Utils, :node_list_all, fn -> [node()] end)
    assert {:ok, parent} = %{} |> SyncWorker.new() |> Oban.insert()
    assert {:ok, duplicate} = %{} |> SyncWorker.new() |> Oban.insert()
    assert duplicate.conflict?
    assert duplicate.id == parent.id

    assert %{success: 1, failure: 0} = Oban.drain_queue(queue: :default)
    assert Repo.reload!(parent).state == "completed"
    assert {:ok, completed_duplicate} = %{} |> SyncWorker.new() |> Oban.insert()
    assert completed_duplicate.conflict?
    assert completed_duplicate.id == parent.id
    assert length(all_enqueued(worker: SyncWorker)) == 1

    parent
    |> Ecto.Changeset.change(inserted_at: DateTime.add(DateTime.utc_now(), -61, :second))
    |> Repo.update!()

    assert {:ok, next_parent} = %{} |> SyncWorker.new() |> Oban.insert()
    refute next_parent.conflict?
    refute next_parent.id == parent.id
  end

  test "child dispatches only a flush command to the named connected node" do
    target = :remote@localhost
    stub(Utils, :node_list_all, fn -> [node(), target] end)

    expect(Utils, :rpc_call, fn ^target, flush, 60_000 ->
      assert flush == (&RecordUsages.flush/0)
      :ok
    end)

    assert :ok = perform_job(SyncWorker, %{"node" => Atom.to_string(target)})
    refute_enqueued(worker: SyncWorker)
  end

  test "an unavailable target cancels without flushing locally or creating an atom" do
    name = "missing-#{System.unique_integer([:positive])}@localhost"
    stub(Utils, :node_list_all, fn -> [node()] end)
    reject(&RecordUsages.flush/0)

    assert {:cancel, :node_unavailable} = perform_job(SyncWorker, %{"node" => name})
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
    refute_enqueued(worker: SyncWorker)
  end

  test "an RPC connection error discards the child without retrying" do
    token = insert(:access_token)
    RecordUsages.record(OauthAccessToken, token.id)
    snapshot = Cache.snapshot()

    stub(Utils, :rpc_call, fn _node, _flush, _timeout ->
      :erlang.error({:erpc, :noconnection})
    end)

    assert {:ok, job} = %{"node" => Atom.to_string(node())} |> SyncWorker.new() |> Oban.insert()
    assert %{success: 0, discard: 1} = Oban.drain_queue(queue: :default, with_safety: true)
    assert %{state: "discarded", attempt: 1} = Repo.reload!(job)
    assert Cache.snapshot() == snapshot
    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
    refute_enqueued(worker: SyncWorker)
  end

  test "a persistence error discards a child and a later cycle can flush the retained usage" do
    token = insert(:access_token)
    Cache.record({OauthAccessToken, token.id}, 1_767_225_600)
    snapshot = Cache.snapshot()
    args = %{"node" => Atom.to_string(node())}
    stub(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert {:ok, job} = args |> SyncWorker.new() |> Oban.insert()
    assert %{discard: 1} = Oban.drain_queue(queue: :default, with_safety: true)
    assert %{state: "discarded", attempt: 1} = Repo.reload!(job)
    assert Cache.snapshot() == snapshot

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert {:ok, _job} = args |> SyncWorker.new() |> Oban.insert()
    assert %{success: 1} = Oban.drain_queue(queue: :default)
    assert Cache.snapshot() == []

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at ==
             ~U[2026-01-01 00:00:00Z]
  end

  test "local child persists token and legacy key usage without changing or evicting either resource" do
    user = insert(:user)
    token = insert(:access_token, id: user.id, resource_owner: user)
    original = Repo.reload!(token)
    original_user = Repo.reload!(user)
    timestamp = ~U[2026-01-01 00:00:00Z]

    assert {:ok, cached_token, _} = Auth.Cache.verify_access_token(token.token, [])
    cached_user = Users.Cache.get_by(api_key: user.api_key)

    refute Ecto.assoc_loaded?(cached_token.usage)
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
    Cache.record({User, user.id}, DateTime.to_unix(timestamp, :second) + 10)

    assert :ok = perform_job(SyncWorker, %{"node" => Atom.to_string(node())})
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(user)

    assert Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Repo.reload!(token) == original
    assert Repo.reload!(user) == original_user
    assert Cache.snapshot() == []

    reject(&Auth.verify_access_token/2)
    reject(&Users.get_by/1)
    assert {:ok, ^cached_token, _} = Auth.Cache.verify_access_token(token.token, [])
    assert Users.Cache.get_by(api_key: user.api_key) == cached_user
  end

  test "flush of equal and older usage does not rewrite rows, while newer usage advances the timestamp" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59Z]
    latest = ~U[2026-01-01 00:00:00Z]
    newer = ~U[2026-01-01 00:00:01Z]

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
    assert :ok = RecordUsages.flush()

    row_query =
      from(usage in RecordUsage,
        where: usage.oauth_access_token_id == ^token.id,
        select: {usage.last_used_at, fragment("ctid::text")}
      )

    original_row = Repo.one!(row_query)
    assert {^latest, _version} = original_row

    for timestamp <- [latest, old] do
      Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
      assert :ok = RecordUsages.flush()
      assert Repo.one!(row_query) == original_row
      assert Cache.snapshot() == []
    end

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(newer, :second))
    assert :ok = RecordUsages.flush()
    assert {^newer, _version} = Repo.one!(row_query)
  end

  test "flush acknowledges only its original snapshot, retaining newer arrivals" do
    user = insert(:user)
    first = insert(:access_token, resource_owner: user)
    second = insert(:access_token, resource_owner: user)
    first_key = {OauthAccessToken, first.id}
    second_key = {OauthAccessToken, second.id}
    timestamp = 1_767_225_600
    Cache.record(first_key, timestamp)
    Cache.record(second_key, timestamp + 10)

    expect(RecordUsages, :persist, fn entries ->
      result = call_original(RecordUsages, :persist, [entries])
      Cache.record(first_key, timestamp + 2)
      Cache.record(first_key, timestamp)
      result
    end)

    assert :ok = RecordUsages.flush()

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: first.id).last_used_at ==
             ~U[2026-01-01 00:00:00Z]

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: second.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Cache.snapshot() == [{first_key, timestamp + 2}]

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: first.id).last_used_at ==
             ~U[2026-01-01 00:00:02Z]
  end

  test "ignores deleted resources and cascades persisted usage when a resource is deleted" do
    for {schema, foreign_key, factory} <- [
          {OauthAccessToken, :oauth_access_token_id, :access_token},
          {User, :legacy_api_key_user_id, :user}
        ] do
      resource = insert(factory)
      deleted_resource = insert(factory)
      RecordUsages.record(schema, resource.id)
      RecordUsages.record(schema, deleted_resource.id)
      Repo.delete!(deleted_resource)

      assert :ok = RecordUsages.flush()
      assert Cache.snapshot() == []
      assert Repo.get_by(RecordUsage, [{foreign_key, deleted_resource.id}]) == nil
      assert Repo.get_by!(RecordUsage, [{foreign_key, resource.id}]).last_used_at != nil

      Repo.delete!(resource)
      assert Repo.get_by(RecordUsage, [{foreign_key, resource.id}]) == nil
    end
  end

  test "legacy usage survives key rotation and an older buffered timestamp" do
    user = insert(:user)
    timestamp = 1_767_225_600
    Cache.record({User, user.id}, timestamp)
    {:ok, user} = Users.update_user_all_fields(user, %{api_key: "rotated-legacy-key"})

    assert :ok = RecordUsages.flush()
    usage = Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id)
    assert usage.last_used_at == ~U[2026-01-01 00:00:00Z]

    Cache.record({User, user.id}, timestamp - 10)
    assert :ok = RecordUsages.flush()
    assert Repo.get_by!(RecordUsage, legacy_api_key_user_id: user.id) == usage
  end

  test "a later flush batch failure retains all original usage for replay" do
    user = insert(:user)
    tokens = insert_list(1_001, :access_token, resource_owner: user)
    timestamp = 1_767_225_600
    Enum.each(tokens, &Cache.record({OauthAccessToken, &1.id}, timestamp))
    snapshot = Enum.sort(Cache.snapshot())

    stub(RecordUsages, :persist, fn entries ->
      if Repo.aggregate(RecordUsage, :count) == 0 do
        assert length(entries) == 1_000
        call_original(RecordUsages, :persist, [entries])
      else
        assert length(entries) == 1
        raise "second batch unavailable"
      end
    end)

    assert_raise RuntimeError, "second batch unavailable", fn ->
      RecordUsages.flush()
    end

    assert Enum.sort(Cache.snapshot()) == snapshot
    assert Repo.aggregate(RecordUsage, :count) == 1_000

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []
    assert Repo.aggregate(RecordUsage, :count) == 1_001
  end

  test "flush with no buffered usage succeeds without persisting anything" do
    reject(&RecordUsages.persist/1)
    assert :ok = RecordUsages.flush()
    assert Cache.snapshot() == []
  end
end
