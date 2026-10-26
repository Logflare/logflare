defmodule Logflare.KeyValues.CacheL2Test do
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L2

  @value %{"org" => %{"id" => "abc"}}
  @telemetry_events [
    [:logflare, :key_values, :cache, :l2, :command, :stop],
    [:logflare, :key_values, :cache, :l2_fill, :stop],
    [:logflare, :key_values, :cache, :db_fallback]
  ]

  setup [:set_mimic_global, :verify_on_exit!]

  setup do
    put_l2_enabled(true)
    start_supervised!(L2)
    [user: insert(:user)]
  end

  test "lookup of a value cached only in L2", %{user: user} do
    reject(&KeyValues.lookup/3)

    for {accessor_path, expected} <- [{nil, @value}, {"org.id", "abc"}] do
      key = "proj:#{accessor_path}"
      :ok = L2.put(lookup_key(user.id, key), @value)

      assert KeyValues.Cache.lookup(user.id, key, accessor_path) == expected
    end
  end

  test "lookup filled from the database", %{user: user} do
    for {accessor_path, expected} <- [{nil, @value}, {"org.id", "abc"}] do
      key = "proj:#{accessor_path}"
      insert(:key_value, user: user, key: key, value: @value)

      assert KeyValues.Cache.lookup(user.id, key, accessor_path) == expected
      assert {:ok, ^expected} = L2.fetch(lookup_key(user.id, key, accessor_path))
    end
  end

  test "lookup served from L2 while its key-value changes", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    :ok = L2.put(lookup_key(user.id, "proj1"), %{"stale" => true})
    stub(L2, :fetch, &Mimic.call_original(L2, :fetch, [&1, &2]))

    expect(L2, :fetch, fn cache_key, opts ->
      hit = Mimic.call_original(L2, :fetch, [cache_key, opts])
      bust(user.id, "proj1")
      hit
    end)

    assert %{"stale" => true} = KeyValues.Cache.lookup(user.id, "proj1")
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
  end

  test "fill read before a busted change of its key-value", %{user: user} do
    for {key, stale} <- [{"updated", %{"v" => "old"}}, {"inserted", nil}] do
      expect(KeyValues, :lookup, fn _user_id, ^key, _accessor_path ->
        bust(user.id, key)
        stale
      end)

      assert KeyValues.Cache.lookup(user.id, key) == stale
      assert {:error, %Nebulex.KeyError{}} = L2.fetch(lookup_key(user.id, key))
    end
  end

  test "busting lookups and accessor variants cached only in L2", %{user: user} do
    for l2_enabled? <- [true, false] do
      key = "proj:#{l2_enabled?}"
      insert(:key_value, user: user, key: key, value: @value)

      for accessor_path <- [nil, "org.id"] do
        :ok = L2.put(lookup_key(user.id, key, accessor_path), :stale)
      end

      put_l2_enabled(l2_enabled?)

      assert {:ok, 2} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: key}])
      put_l2_enabled(true)
      assert @value = KeyValues.Cache.lookup(user.id, key)
      assert "abc" = KeyValues.Cache.lookup(user.id, key, "org.id")
    end
  end

  test "concurrent lookups of an uncached key", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)

    expect(L2, :fetch, fn cache_key, opts ->
      Process.sleep(100)
      Mimic.call_original(L2, :fetch, [cache_key, opts])
    end)

    expect(KeyValues, :lookup, &Mimic.call_original(KeyValues, :lookup, [&1, &2, &3]))

    results =
      1..10
      |> Task.async_stream(fn _ -> KeyValues.Cache.lookup(user.id, "proj1") end)
      |> Enum.map(fn {:ok, value} -> value end)

    assert results == List.duplicate(@value, 10)
  end

  test "lookup while L2 is failing", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    reject(&L2.find_node/1)

    for {failure, accessor_path, expected} <- [
          {:timeout, nil, @value},
          {:timeout, "org.id", "abc"},
          {:remote_raise, nil, @value},
          {:remote_raise, "org.id", "abc"},
          {:stopped, nil, @value},
          {:stopped, "org.id", "abc"}
        ] do
      KeyValues.Cache.reset()
      expect(L2, :fetch, fn _cache_key, _opts -> fail(failure) end)

      assert KeyValues.Cache.lookup(user.id, "proj1", accessor_path) == expected
    end
  end

  test "lookup with L2 disabled at runtime", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    :ok = L2.put(lookup_key(user.id, "proj1"), %{"stale" => true})
    put_l2_enabled(false)

    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
  end

  test "telemetry of lookups", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    ref = :telemetry_test.attach_event_handlers(self(), @telemetry_events)
    on_exit(fn -> :telemetry.detach(ref) end)

    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")

    assert_receive {[_, _, _, :l2, :command, :stop], ^ref, _, %{command: :fetch, result: miss}}
    assert :miss = L2.outcome(miss)
    assert_receive {[_, _, _, :db_fallback], ^ref, %{count: 1}, %{reason: :miss}}
    assert_receive {[_, _, _, :l2_fill, :stop], ^ref, %{duration: _}, %{result: :put}}
    refute_receive {_event, ^ref, _measurements, _metadata}
  end

  test "count with L2 enabled", %{user: user} do
    insert(:key_value, user: user, key: "proj1")
    reject(&L2.fetch/2)

    assert 1 = KeyValues.Cache.count(user.id)
  end

  defp bust(user_id, key) do
    {:noreply, _state} =
      CacheBusterWorker.handle_cast(
        {:to_bust, [{KeyValues, user_id: user_id, key: key}], []},
        %{}
      )
  end

  defp fail(:timeout),
    do: {:error, Nebulex.Error.exception(reason: {:rpc, {:error, :timeout}})}

  defp fail(:remote_raise), do: raise(RuntimeError, "RPC runtime error")
  defp fail(:stopped), do: raise(Nebulex.CacheNotFoundError, cache: L2)

  defp lookup_key(user_id, key, accessor_path \\ nil),
    do: {:lookup, [user_id, key, accessor_path]}

  defp put_l2_enabled(enabled?) do
    original = Application.get_env(:logflare, KeyValues.Cache)
    Application.put_env(:logflare, KeyValues.Cache, Keyword.put(original, :l2_enabled, enabled?))
    on_exit(fn -> Application.put_env(:logflare, KeyValues.Cache, original) end)
  end
end
