defmodule Logflare.ContextCache.CacheBusterWalTest do
  use ExUnit.Case, async: false

  import Logflare.Factory

  alias Cainophile.Changes.NewRecord
  alias Cainophile.Changes.Transaction
  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.ContextCache
  alias Logflare.ContextCache.TransactionBroadcaster
  alias Logflare.KeyValues
  alias Logflare.Repo
  alias Logflare.TestUtils
  alias Logflare.User

  setup do
    for child_spec <- ContextCache.Supervisor.buster_specs() do
      start_supervised!(child_spec)
    end

    start_supervised!({TransactionBroadcaster, interval: 100})

    TestUtils.retry_assert(fn ->
      assert :sys.get_state(TransactionBroadcaster).subscribed_pid != nil
    end)

    Phoenix.PubSub.subscribe(Logflare.PubSub, "wal_transactions")

    {:ok, kv} =
      Sandbox.unboxed_run(Repo, fn ->
        user = insert(:user)
        {:ok, insert(:key_value, user: user, key: "proj", value: %{"org" => "a"})}
      end)

    assert_receive %Transaction{changes: [%NewRecord{relation: {_, "key_values"}} | _]}, 5_000
    Process.sleep(200)

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn ->
        Repo.delete_all(Logflare.KeyValues.KeyValue)
        Repo.delete(Repo.get(User, kv.user_id))
      end)
    end)

    [kv: kv]
  end

  test "deleting a key_values row through the real WAL stream busts its cached lookups", %{kv: kv} do
    Cachex.put!(KeyValues.Cache, {:lookup, [kv.user_id, "proj", nil]}, {:cached, kv.value})
    Cachex.put!(KeyValues.Cache, {:lookup, [kv.user_id, "proj", "org"]}, {:cached, "a"})

    Sandbox.unboxed_run(Repo, fn -> Repo.delete!(kv) end)

    TestUtils.retry_assert(fn ->
      assert Cachex.get!(KeyValues.Cache, {:lookup, [kv.user_id, "proj", nil]}) == nil
      assert Cachex.get!(KeyValues.Cache, {:lookup, [kv.user_id, "proj", "org"]}) == nil
    end)
  end

  test "inserting a key_values row through the real WAL stream makes it visible", %{kv: kv} do
    KeyValues.KeyFilter.rebuild(0, fn _filter -> :ok end)
    on_exit(&KeyValues.KeyFilter.clear/0)

    Sandbox.unboxed_run(Repo, fn ->
      user = Repo.get!(User, kv.user_id)
      insert(:key_value, user: user, key: "new", value: %{"org" => "c"})
    end)

    TestUtils.retry_assert(fn ->
      assert KeyValues.KeyFilter.maybe_exists?(kv.user_id, "new")
    end)

    Sandbox.unboxed_run(Repo, fn ->
      assert KeyValues.Cache.lookup(kv.user_id, "new") == %{"org" => "c"}
    end)
  end
end
