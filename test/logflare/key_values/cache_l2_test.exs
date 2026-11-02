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
    enable_l2()
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
    ref = attach_l2_fill()

    for {accessor_path, expected} <- [{nil, @value}, {"org.id", "abc"}] do
      key = "proj:#{accessor_path}"
      insert(:key_value, user: user, key: key, value: @value)

      assert KeyValues.Cache.lookup(user.id, key, accessor_path) == expected
      assert_receive {_event, ^ref, _measurements, %{result: :put}}
      assert {:ok, @value} = L2.fetch(lookup_key(user.id, key))
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
    ref = attach_l2_fill()

    for {key, stale} <- [{"updated", %{"v" => "old"}}, {"inserted", nil}] do
      expect(KeyValues, :lookup, fn _user_id, ^key, _accessor_path ->
        bust(user.id, key)
        stale
      end)

      assert KeyValues.Cache.lookup(user.id, key) == stale
      assert_receive {_event, ^ref, _measurements, %{result: :dropped_stale}}
      assert {:error, %Nebulex.KeyError{}} = L2.fetch(lookup_key(user.id, key))
    end
  end

  test "busting a key-value cached only in L2", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    :ok = L2.put(lookup_key(user.id, "proj1"), %{"org" => %{"id" => "stale"}})

    assert {:ok, 1} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "proj1"}])
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    assert "abc" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")
  end

  @tag :capture_log
  test "lookup while L2 is failing", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    reject(&L2.put/3)

    for failure <- [:timeout, :raise, :exit] do
      KeyValues.Cache.reset()
      expect(L2, :fetch, fn _cache_key, _opts -> fail(failure) end)

      assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    end
  end

  test "busting a key-value cached in L1 and L2", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    ref = attach_l2_fill()
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    assert_receive {_event, ^ref, _measurements, %{result: :put}}

    assert {:ok, 1} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "proj1"}])
  end

  @tag :capture_log
  test "busting while L2 fails", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")

    for failure <- [:raise, :exit] do
      stub(L2, :take, fn _cache_key, _opts -> fail(failure) end)

      assert {:ok, _busted} =
               ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "proj1"}])

      assert {:ok, nil} = Cachex.get(KeyValues.Cache, lookup_key(user.id, "proj1"))
    end
  end

  test "reset with entries in L1 and L2", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    ref = attach_l2_fill()
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    assert_receive {_event, ^ref, _measurements, %{result: :put}}

    assert :ok = KeyValues.Cache.reset()

    assert {:ok, nil} = Cachex.get(KeyValues.Cache, lookup_key(user.id, "proj1"))
    assert {:error, %Nebulex.KeyError{}} = L2.fetch(lookup_key(user.id, "proj1"))
  end

  test "telemetry of lookups", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @value)
    ref = :telemetry_test.attach_event_handlers(self(), @telemetry_events)
    on_exit(fn -> :telemetry.detach(ref) end)

    assert @value = KeyValues.Cache.lookup(user.id, "proj1")
    assert @value = KeyValues.Cache.lookup(user.id, "proj1")

    assert_receive {[_, _, _, :l2, :command, :stop], ^ref, _,
                    %{command: :fetch, result: {:error, %Nebulex.KeyError{}}}}

    assert_receive {[_, _, _, :db_fallback], ^ref, %{count: 1}, %{reason: :miss}}
    assert_receive {[_, _, _, :l2_fill, :stop], ^ref, %{duration: _}, %{result: :put}}
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

  defp fail(:raise), do: raise(Nebulex.CacheNotFoundError, cache: L2)

  defp fail(:exit), do: exit({:noproc, {GenServer, :call, [L2, :get_state, 5_000]}})

  defp attach_l2_fill do
    ref = :telemetry_test.attach_event_handlers(self(), [Enum.at(@telemetry_events, 1)])
    on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end

  defp lookup_key(user_id, key, accessor_path \\ nil),
    do: {:lookup, [user_id, key, accessor_path]}

  defp enable_l2 do
    original = Application.get_env(:logflare, KeyValues.Cache)
    Application.put_env(:logflare, KeyValues.Cache, Keyword.put(original, :l2_enabled, true))
    on_exit(fn -> Application.put_env(:logflare, KeyValues.Cache, original) end)
  end
end
