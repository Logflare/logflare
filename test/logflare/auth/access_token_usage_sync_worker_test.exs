defmodule Logflare.Auth.AccessTokenUsageSyncWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Auth
  alias Logflare.Auth.AccessTokenUsageSyncWorker
  alias Logflare.Auth.UsageCache
  alias Logflare.Cluster.Utils
  alias Logflare.ContextCache.Supervisor, as: CacheSupervisor
  alias Logflare.OauthAccessTokens.OauthAccessTokenUsage

  test "persists user and partner usage without changing tokens or evicting authentication" do
    user = insert(:user)
    partner = insert(:partner)
    {:ok, token} = Auth.create_access_token(user)
    {:ok, partner_token} = Auth.create_access_token(partner)
    original = Repo.reload!(token)
    timestamp = ~U[2026-01-01 00:00:00.000000Z]

    assert {:ok, cached_token, _} = Auth.Cache.verify_access_token(token.token, [])

    assert {:ok, cached_partner_token, _} =
             Auth.Cache.verify_access_token(partner_token.token, ["partner"])

    refute Ecto.assoc_loaded?(cached_token.usage)
    refute Ecto.assoc_loaded?(cached_partner_token.usage)
    UsageCache.record(token.id, timestamp)
    UsageCache.record(partner_token.id, timestamp)

    assert :ok = perform_job(AccessTokenUsageSyncWorker, %{})
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(user)
    assert [%{usage: %{last_used_at: ^timestamp}}] = Auth.list_valid_access_tokens(partner)
    assert Repo.reload!(token) == original
    assert UsageCache.snapshot() == []

    for {value, scopes} <- [{token.token, []}, {partner_token.token, ["partner"]}] do
      assert {:ok, verified_token, _} = Auth.verify_access_token(value, scopes)
      refute Ecto.assoc_loaded?(verified_token.usage)
    end

    reject(&Auth.verify_access_token/2)
    assert {:ok, ^cached_token, _} = Auth.Cache.verify_access_token(token.token, [])

    assert {:ok, ^cached_partner_token, _} =
             Auth.Cache.verify_access_token(partner_token.token, ["partner"])
  end

  test "ignores deleted tokens and cascades persisted usage when a token is deleted" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    deleted_token = insert(:access_token, resource_owner: user)
    Auth.record_access_token_usage(token)
    Auth.record_access_token_usage(deleted_token)
    Repo.delete!(deleted_token)

    assert :ok = perform_job(AccessTokenUsageSyncWorker, %{})
    assert UsageCache.snapshot() == []
    assert Repo.get(OauthAccessTokenUsage, deleted_token.id) == nil
    assert Repo.get!(OauthAccessTokenUsage, token.id).last_used_at != nil

    Repo.delete!(token)
    assert Repo.get(OauthAccessTokenUsage, token.id) == nil
  end

  test "acknowledgement preserves newer usage received during persistence" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    old = ~U[2025-12-31 23:59:59.000000Z]
    latest = ~U[2026-01-01 00:00:00.000000Z]
    UsageCache.record(token.id, old)

    stub(Auth, :persist_access_token_usage, fn entries ->
      result = call_original(Auth, :persist_access_token_usage, [entries])
      UsageCache.record(token.id, latest)
      UsageCache.record(token.id, old)
      result
    end)

    assert :ok = perform_job(AccessTokenUsageSyncWorker, %{})
    assert [%{usage: %{last_used_at: ^old}}] = Auth.list_valid_access_tokens(user)
    assert UsageCache.snapshot() == [{token.id, latest}]

    stub(
      Auth,
      :persist_access_token_usage,
      &call_original(Auth, :persist_access_token_usage, [&1])
    )

    assert :ok = perform_job(AccessTokenUsageSyncWorker, %{})
    assert [%{usage: %{last_used_at: ^latest}}] = Auth.list_valid_access_tokens(user)
    assert UsageCache.snapshot() == []
  end

  test "failed persistence leaves usage available for retry" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    Auth.record_access_token_usage(token)
    snapshot = UsageCache.snapshot()

    stub(Auth, :persist_access_token_usage, fn _entries -> raise "database unavailable" end)

    assert_raise RuntimeError, "database unavailable", fn ->
      perform_job(AccessTokenUsageSyncWorker, %{})
    end

    assert UsageCache.snapshot() == snapshot
    assert [%{usage: nil}] = Auth.list_valid_access_tokens(user)

    stub(
      Auth,
      :persist_access_token_usage,
      &call_original(Auth, :persist_access_token_usage, [&1])
    )

    assert :ok = perform_job(AccessTokenUsageSyncWorker, %{})
    assert UsageCache.snapshot() == []
    assert [%{usage: %{last_used_at: %DateTime{}}}] = Auth.list_valid_access_tokens(user)
  end

  test "an unreachable node does not prevent flushing reachable nodes" do
    user = insert(:user)
    token = insert(:access_token, resource_owner: user)
    Auth.record_access_token_usage(token)

    stub(Utils, :node_list_all, fn -> [:"unreachable@127.0.0.1", node()] end)

    assert {:error, [_]} = perform_job(AccessTokenUsageSyncWorker, %{})
    assert [%{usage: %{last_used_at: %DateTime{}}}] = Auth.list_valid_access_tokens(user)
    assert UsageCache.snapshot() == []
  end

  test "a node lost during acknowledgement does not prevent flushing remaining nodes" do
    user = insert(:user)
    remote_token = insert(:access_token, resource_owner: user)
    local_token = insert(:access_token, resource_owner: user)
    timestamp = ~U[2026-01-01 00:00:00.000000Z]
    UsageCache.record(local_token.id, timestamp)

    stub(Utils, :erpc_multicall, fn
      _nodes, UsageCache, :snapshot, [] ->
        [
          {:"unreachable@127.0.0.1", {:ok, [{remote_token.id, timestamp}]}},
          {node(), {:ok, UsageCache.snapshot()}}
        ]

      nodes, mod, function, args ->
        call_original(Utils, :erpc_multicall, [nodes, mod, function, args])
    end)

    assert {:error, [_]} = perform_job(AccessTokenUsageSyncWorker, %{})

    assert Enum.map(Auth.list_valid_access_tokens(user), & &1.usage.last_used_at) == [
             timestamp,
             timestamp
           ]

    assert UsageCache.snapshot() == []
  end

  test "usage cache supports stats when cache metrics are enabled" do
    previous = Application.fetch_env!(:logflare, :cache_stats)
    :ok = Supervisor.terminate_child(CacheSupervisor, UsageCache)

    on_exit(fn ->
      Application.put_env(:logflare, :cache_stats, previous)
      {:ok, _} = Supervisor.restart_child(CacheSupervisor, UsageCache)
    end)

    Application.put_env(:logflare, :cache_stats, true)
    start_supervised!(UsageCache)

    assert {:ok, stats} = Cachex.stats(UsageCache)
    assert is_map(stats)
  end
end
