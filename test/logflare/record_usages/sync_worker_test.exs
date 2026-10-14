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

  test "persists usage without changing tokens or evicting authentication" do
    user = insert(:user)
    {:ok, token} = Auth.create_access_token(user)
    original = Repo.reload!(token)
    timestamp = ~U[2026-01-01 00:00:00Z]

    assert {:ok, cached_token, _} = Auth.Cache.verify_access_token(token.token, [])

    refute Ecto.assoc_loaded?(cached_token.usage)
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))

    assert :ok = perform_job(SyncWorker, %{})
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(user)
    assert Repo.reload!(token) == original
    assert Cache.snapshot() == []

    reject(&Auth.verify_access_token/2)
    assert {:ok, ^cached_token, _} = Auth.Cache.verify_access_token(token.token, [])
  end

  test "equal and older usage do not rewrite rows, while newer usage advances the timestamp" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59Z]
    latest = ~U[2026-01-01 00:00:00Z]
    newer = ~U[2026-01-01 00:00:01Z]

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
    assert :ok = perform_job(SyncWorker, %{})

    row_query =
      from(usage in RecordUsage,
        where: usage.oauth_access_token_id == ^token.id,
        select: {usage.last_used_at, fragment("ctid::text")}
      )

    original_row = Repo.one!(row_query)
    assert {^latest, _version} = original_row

    for timestamp <- [latest, old] do
      Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))
      assert :ok = perform_job(SyncWorker, %{})
      assert Repo.one!(row_query) == original_row
      assert Cache.snapshot() == []
    end

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(newer, :second))
    assert :ok = perform_job(SyncWorker, %{})
    assert {^newer, _version} = Repo.one!(row_query)
  end

  test "merges overlapping nodes once per resource and acknowledges original snapshots" do
    user = insert(:user)
    first = insert(:access_token, resource_owner: user)
    second = insert(:access_token, resource_owner: user)
    first_key = {OauthAccessToken, first.id}
    second_key = {OauthAccessToken, second.id}
    timestamp = 1_767_225_600
    Cache.record(first_key, timestamp)
    Cache.record(second_key, timestamp + 10)

    remote_snapshot = [{first_key, timestamp + 2}, {second_key, timestamp + 3}]

    stub(Utils, :erpc_multicall, fn
      _nodes, Cache, :snapshot, [] ->
        [{node(), {:ok, Cache.snapshot()}}, {:"remote@127.0.0.1", {:ok, remote_snapshot}}]

      [remote], Cache, :acknowledge, [entries] when remote != node() ->
        assert entries == remote_snapshot
        [{remote, {:ok, :ok}}]

      nodes, mod, function, args ->
        call_original(Utils, :erpc_multicall, [nodes, mod, function, args])
    end)

    expect(RecordUsages, :persist, fn entries ->
      assert Enum.sort(entries) ==
               Enum.sort([{first_key, timestamp + 2}, {second_key, timestamp + 10}])

      result = call_original(RecordUsages, :persist, [entries])
      Cache.record(first_key, timestamp + 2)
      Cache.record(first_key, timestamp)
      result
    end)

    assert :ok = perform_job(SyncWorker, %{})

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: first.id).last_used_at ==
             ~U[2026-01-01 00:00:02Z]

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: second.id).last_used_at ==
             ~U[2026-01-01 00:00:10Z]

    assert Cache.snapshot() == [{first_key, timestamp + 2}]
  end

  test "ignores deleted tokens and cascades persisted usage when a token is deleted" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    deleted_token = insert(:access_token, resource_owner: user)
    RecordUsages.record(OauthAccessToken, token.id)
    RecordUsages.record(OauthAccessToken, deleted_token.id)
    Repo.delete!(deleted_token)

    assert :ok = perform_job(SyncWorker, %{})
    assert Cache.snapshot() == []
    assert Repo.get_by(RecordUsage, oauth_access_token_id: deleted_token.id) == nil
    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at != nil

    Repo.delete!(token)
    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
  end

  test "a later merged batch failure retains all original usage for retry" do
    user = insert(:user)
    tokens = insert_list(1_001, :access_token, resource_owner: user)
    timestamp = 1_767_225_600
    Enum.each(tokens, &Cache.record({OauthAccessToken, &1.id}, timestamp))
    snapshot = Enum.sort(Cache.snapshot())

    stub(Utils, :erpc_multicall, fn
      _nodes, Cache, :snapshot, [] ->
        [{node(), {:ok, Cache.snapshot()}}, {:"remote@127.0.0.1", {:ok, snapshot}}]

      [remote], Cache, :acknowledge, [_entries] when remote != node() ->
        [{remote, {:ok, :ok}}]

      nodes, mod, function, args ->
        call_original(Utils, :erpc_multicall, [nodes, mod, function, args])
    end)

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
      perform_job(SyncWorker, %{})
    end

    assert Enum.sort(Cache.snapshot()) == snapshot
    assert Repo.aggregate(RecordUsage, :count) == 1_000

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = perform_job(SyncWorker, %{})
    assert Cache.snapshot() == []
    assert Repo.aggregate(RecordUsage, :count) == 1_001
  end

  test "failed acknowledgement retains persisted usage until a successful retry" do
    token = insert(:access_token)
    Cache.record({OauthAccessToken, token.id}, 1_767_225_600)
    snapshot = Cache.snapshot()

    stub(Utils, :erpc_multicall, fn
      [local], Cache, :acknowledge, [_entries] when local == node() ->
        [{local, {:error, {:erpc, :timeout}}}]

      nodes, mod, function, args ->
        call_original(Utils, :erpc_multicall, [nodes, mod, function, args])
    end)

    assert {:error, [{:error, {local, {:error, {:erpc, :timeout}}}}]} =
             perform_job(SyncWorker, %{})

    assert local == node()
    assert Cache.snapshot() == snapshot

    row_version = from(usage in RecordUsage, select: {usage.last_used_at, fragment("ctid::text")})
    persisted = Repo.one!(row_version)
    assert {~U[2026-01-01 00:00:00Z], _version} = persisted

    stub(Utils, :erpc_multicall, &call_original(Utils, :erpc_multicall, [&1, &2, &3, &4]))

    assert :ok = perform_job(SyncWorker, %{})
    assert Cache.snapshot() == []
    assert Repo.one!(row_version) == persisted
  end

  test "snapshot and acknowledgement failures do not prevent flushing remaining nodes" do
    user = insert(:user)
    remote_token = insert(:access_token, resource_owner: user)
    local_token = insert(:access_token, resource_owner: user)
    timestamp = ~U[2026-01-01 00:00:00Z]
    timestamp_seconds = DateTime.to_unix(timestamp, :second)
    Cache.record({OauthAccessToken, local_token.id}, timestamp_seconds)

    stub(Utils, :erpc_multicall, fn
      _nodes, Cache, :snapshot, [] ->
        [
          {:"offline@127.0.0.1", {:error, {:erpc, :noconnection}}},
          {:"unreachable@127.0.0.1",
           {:ok, [{{OauthAccessToken, remote_token.id}, timestamp_seconds}]}},
          {node(), {:ok, Cache.snapshot()}}
        ]

      nodes, mod, function, args ->
        call_original(Utils, :erpc_multicall, [nodes, mod, function, args])
    end)

    assert {:error,
            [
              {:error, {:"offline@127.0.0.1", _}},
              {:error, {:"unreachable@127.0.0.1", _}}
            ]} = perform_job(SyncWorker, %{})

    assert Enum.map(Auth.list_valid_access_tokens(user), & &1.usage.last_used_at) == [
             timestamp,
             timestamp
           ]

    assert Cache.snapshot() == []
  end
end
