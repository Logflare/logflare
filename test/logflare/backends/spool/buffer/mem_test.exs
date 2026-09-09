defmodule Logflare.Backends.Spool.Buffer.MemTest do
  use ExUnit.Case, async: false

  alias Logflare.Backends.Spool.Buffer.Mem
  alias Logflare.Backends.Spool.Framing

  setup do
    on_exit(fn -> Application.delete_env(:logflare, :spool) end)
    :ok
  end

  defp segment(payload \\ "line\n"), do: Framing.encode_segment(payload)

  describe "init/1" do
    test "defaults max_batch_bytes to 7MB when unset" do
      state = Mem.init([])
      assert state.max_batch_bytes == 7 * 1024 * 1024
      assert state.pending == []
      assert state.pending_bytes == 0
      assert state.pending_count == 0
    end

    test "reads mem_max_batch_bytes from config" do
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 42)
      assert Mem.init([]).max_batch_bytes == 42
    end
  end

  describe "append/4" do
    test "accumulates segments and tracks pending_bytes/pending_count across calls" do
      state = Mem.init([])

      assert {:ok, state} = Mem.append(state, segment(), 10, 1)
      assert state.pending_bytes == 10
      assert state.pending_count == 1

      assert {:ok, state} = Mem.append(state, segment(), 5, 1)
      assert state.pending_bytes == 15
      assert state.pending_count == 2
    end
  end

  describe "roll/2" do
    test "is a no-op when nothing is pending, regardless of force" do
      state = Mem.init([])

      assert {:no_roll, ^state} = Mem.roll(state, false)
      assert {:no_roll, ^state} = Mem.roll(state, true)
    end

    test "does not roll under the byte threshold without force" do
      state = Mem.init([])
      {:ok, state} = Mem.append(state, segment(), 10, 1)

      assert {:no_roll, ^state} = Mem.roll(state, false)
    end

    test "rolls when force: true, however little has accumulated" do
      state = Mem.init([])
      {:ok, state} = Mem.append(state, segment(), 10, 1)

      assert {:ok, thunk, nil, 1, new_state} = Mem.roll(state, true)
      assert new_state.pending == []
      assert new_state.pending_bytes == 0
      assert new_state.pending_count == 0
      assert {:ok, body} = thunk.()
      assert {:ok, ["line\n"]} = Framing.decode_segments(body)
    end

    test "rolls once the configured byte threshold is crossed" do
      Application.put_env(:logflare, :spool, mem_max_batch_bytes: 5)
      state = Mem.init([])

      {:ok, state} = Mem.append(state, segment(), 10, 1)

      assert {:ok, _thunk, nil, 1, _new_state} = Mem.roll(state, false)
    end

    test "concatenates multiple appended segments in the order they were appended" do
      state = Mem.init([])
      {:ok, state} = Mem.append(state, segment("first\n"), 6, 1)
      {:ok, state} = Mem.append(state, segment("second\n"), 7, 1)
      {:ok, state} = Mem.append(state, segment("third\n"), 6, 1)

      assert {:ok, thunk, nil, 3, _new_state} = Mem.roll(state, true)
      assert {:ok, body} = thunk.()
      assert {:ok, ["first\n", "second\n", "third\n"]} = Framing.decode_segments(body)
    end
  end

  describe "on_commit_result/3" do
    test "is a no-op, regardless of the result" do
      state = Mem.init([])
      assert ^state = Mem.on_commit_result(state, nil, :ok)
      assert ^state = Mem.on_commit_result(state, nil, {:error, :timeout})
    end
  end

  describe "recover/1" do
    test "always returns no items — there's nothing durable here to find" do
      state = Mem.init([])
      assert {[], ^state} = Mem.recover(state)
    end
  end
end
