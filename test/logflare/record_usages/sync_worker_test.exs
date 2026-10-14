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

  test "flushing older usage does not overwrite a newer persisted timestamp" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59Z]
    latest = ~U[2026-01-01 00:00:00Z]

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
    assert :ok = perform_job(SyncWorker, %{})
    assert [%{usage: %{last_used_at: ^latest}}] = Auth.list_valid_access_tokens(user)

    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(old, :second))

    assert Cache.snapshot() == [
             {{OauthAccessToken, token.id}, DateTime.to_unix(old, :second)}
           ]

    assert :ok = perform_job(SyncWorker, %{})
    assert [%{usage: %{last_used_at: ^latest}}] = Auth.list_valid_access_tokens(user)
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

  test "acknowledgement preserves newer usage received during persistence" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59Z]
    latest = ~U[2026-01-01 00:00:00Z]
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(old, :second))

    stub(RecordUsages, :persist, fn entries ->
      result = call_original(RecordUsages, :persist, [entries])
      Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(latest, :second))
      Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(old, :second))
      result
    end)

    assert :ok = perform_job(SyncWorker, %{})
    assert [%{usage: %{last_used_at: ^old}}] = Auth.list_valid_access_tokens(user)

    assert Cache.snapshot() == [
             {{OauthAccessToken, token.id}, DateTime.to_unix(latest, :second)}
           ]

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = perform_job(SyncWorker, %{})
    assert [%{usage: %{last_used_at: ^latest}}] = Auth.list_valid_access_tokens(user)
    assert Cache.snapshot() == []
  end

  test "failed persistence leaves usage available for retry" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    RecordUsages.record(OauthAccessToken, token.id)
    snapshot = Cache.snapshot()

    stub(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert_raise RuntimeError, "database unavailable", fn ->
      perform_job(SyncWorker, %{})
    end

    assert Cache.snapshot() == snapshot
    assert [%{usage: nil}] = Auth.list_valid_access_tokens(user)

    stub(RecordUsages, :persist, &call_original(RecordUsages, :persist, [&1]))

    assert :ok = perform_job(SyncWorker, %{})
    assert Cache.snapshot() == []
    assert [%{usage: %{last_used_at: %DateTime{}}}] = Auth.list_valid_access_tokens(user)
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
