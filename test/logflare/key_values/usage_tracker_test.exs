defmodule Logflare.KeyValues.UsageTrackerTest do
  use Logflare.DataCase, async: false

  alias Logflare.KeyValues.KeyValueUsage
  alias Logflare.KeyValues.UsageTracker

  @buffers {:key_value_usage_buffer_0, :key_value_usage_buffer_1}
  @active_idx_key {UsageTracker, :active_idx_ref}

  setup do
    # The tracker is not supervised in :test, so start it per-test with a long
    # interval (flushing is driven explicitly via flush/0) and a small chunk
    # size so the drain's continuation loop is exercised.
    start_supervised!({UsageTracker, flush_interval: :timer.hours(24), flush_chunk_size: 5})
    user = insert(:user)
    kv = insert(:key_value, user: user, key: "k1")
    [user: user, kv: kv]
  end

  describe "touch/2" do
    test "inserts an entry into the active buffer", %{user: user} do
      assert :ok = UsageTracker.touch(user.id, "k1")
      assert [{{_uid, "k1"}}] = :ets.lookup(active_table(), {user.id, "k1"})
    end

    test "dedups repeated touches of the same key", %{user: user} do
      UsageTracker.touch(user.id, "k1")
      UsageTracker.touch(user.id, "k1")
      UsageTracker.touch(user.id, "k1")

      assert 1 = length(:ets.lookup(active_table(), {user.id, "k1"}))
    end

    test "is safe when the buffer tables are absent", %{user: user} do
      # Stopping the tracker drops the tables it owns.
      stop_supervised!(UsageTracker)

      assert :ok = UsageTracker.touch(user.id, "gone")
    end
  end

  describe "flush/0" do
    test "repeated key", %{user: user, kv: kv} do
      drained = active_table()
      UsageTracker.touch(user.id, "k1")
      assert :ok = UsageTracker.flush()
      assert [%KeyValueUsage{key_value_id: kv_id, last_used_at: first}] = Repo.all(KeyValueUsage)
      assert kv_id == kv.id
      assert [] = :ets.lookup(drained, {user.id, "k1"})

      UsageTracker.touch(user.id, "k1")
      assert :ok = UsageTracker.flush()

      assert [%KeyValueUsage{key_value_id: kv_id, last_used_at: second}] = Repo.all(KeyValueUsage)
      assert kv_id == kv.id
      assert DateTime.compare(second, first) in [:gt, :eq]
    end

    test "empty buffer" do
      assert :ok = UsageTracker.flush()
      assert [] = Repo.all(KeyValueUsage)
    end

    test "deleted key-values", %{user: user} do
      UsageTracker.touch(user.id, "never_existed")

      assert :ok = UsageTracker.flush()
      assert [] = Repo.all(KeyValueUsage)
    end

    test "drain in multiple chunks", %{user: user} do
      # flush_chunk_size is 5 in test config, so 12 keys exercises the
      # :ets.select/3 continuation loop across three chunks.
      keys = for i <- 1..12, do: "kv#{i}"

      Enum.each(keys, fn key ->
        insert(:key_value, user: user, key: key)
        UsageTracker.touch(user.id, key)
      end)

      assert :ok = UsageTracker.flush()

      assert 12 = Repo.aggregate(KeyValueUsage, :count)
    end

    test "table rotation", %{user: user, kv: kv1} do
      kv2 = insert(:key_value, user: user, key: "k2")

      UsageTracker.touch(user.id, "k1")
      assert :ok = UsageTracker.flush()

      assert [%KeyValueUsage{}] = Repo.all(KeyValueUsage)

      # This touch lands in the now-active (flipped) table.
      UsageTracker.touch(user.id, "k2")
      assert :ok = UsageTracker.flush()

      usages = Repo.all(KeyValueUsage) |> Enum.map(fn %{key_value_id: id} -> id end)
      assert length(usages) == 2
      assert kv1.id in usages
      assert kv2.id in usages
    end
  end

  describe "terminate/2" do
    test "flush on shutdown", %{user: user} do
      UsageTracker.touch(user.id, "k1")

      # Graceful shutdown runs terminate/2, which flushes before exiting.
      stop_supervised!(UsageTracker)

      assert [%KeyValueUsage{}] = Repo.all(KeyValueUsage)
    end
  end

  defp active_table do
    ref = :persistent_term.get(@active_idx_key)
    elem(@buffers, :atomics.get(ref, 1))
  end
end
