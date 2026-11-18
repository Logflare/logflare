defmodule Logflare.RecordUsagesTest do
  use ExUnit.Case, async: false

  import Cachex.Spec, only: [name: 2]
  import Ecto.Query
  import Logflare.Factory

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.Writer
  alias Logflare.Repo
  alias Logflare.TestUtils
  alias Logflare.User

  test "read-first recordings coalesce pending maxima without waiting for writes" do
    Cache.reset()
    Cache.record({User, 1}, 101)
    Cache.record({User, 2}, 2)
    assert Map.new(Cache.snapshot()) == %{{User, 1} => 101, {User, 2} => 2}

    queue = Process.whereis(name(Cache, :locksmith))
    writer = Process.whereis(Writer)

    on_exit(fn ->
      :sys.resume(queue)
      :sys.statistics(queue, false)
      if Process.alive?(writer), do: :sys.resume(writer)
      Cache.reset()
    end)

    :sys.statistics(queue, true)
    :sys.suspend(queue)
    :sys.suspend(writer)

    for timestamp <- [101, 99] do
      assert :ok = Cache.record({User, 1}, timestamp)
    end

    {:messages, messages} = Process.info(writer, :messages)
    refute Enum.any?(messages, &match?({:"$gen_cast", {:record, _, _}}, &1))
    :sys.resume(writer)

    assert {:message_queue_len, 0} = Process.info(queue, :message_queue_len)
    assert :ok = Cache.record({User, 2}, 3)

    TestUtils.retry_assert(fn ->
      assert {:message_queue_len, 1} = Process.info(queue, :message_queue_len)
    end)

    1..1_000
    |> Task.async_stream(fn timestamp -> Cache.record({User, 2}, timestamp) end, timeout: 1_000)
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert :ok = Cache.record({User, 3}, 7)
    assert :ok = Cache.record({User, 2}, 9)
    assert {:message_queue_len, 1} = Process.info(queue, :message_queue_len)
    assert {:ok, 2} = Cachex.get(Cache, {User, 2})
    assert {:ok, nil} = Cachex.get(Cache, {User, 3})

    :sys.resume(queue)
    assert Map.new(Cache.snapshot()) == %{{User, 1} => 101, {User, 2} => 1_000, {User, 3} => 7}
    assert {:ok, statistics} = :sys.statistics(queue, :get)
    assert statistics[:messages_in] <= 3

    Writer.record({User, 2}, 9)
    assert Map.fetch!(Map.new(Cache.snapshot()), {User, 2}) == 1_000
  end

  test "drains finish their current or pending batch without chasing later arrivals" do
    Cache.reset()
    on_exit(fn -> Cache.reset() end)
    queue = Process.whereis(name(Cache, :locksmith))
    initial = queue_gate(queue)
    await_gate(initial, queue)
    Cache.record({User, 1}, 13)
    queued_write(queue)
    current = Task.async(&Writer.drain/0)
    await_drain(current)

    Cache.record({User, 2}, 47)
    pending = Task.async(&Writer.drain/0)
    await_drain(pending)
    next = queue_gate(queue)
    await_gate_queued(next, queue)
    release_gate(initial, queue)
    await_gate(next, queue)
    assert :ok = Task.await(current, 1_000)
    assert Task.yield(pending, 0) == nil

    queued_write(queue)
    Cache.record({User, 3}, 79)
    last = queue_gate(queue)
    await_gate_queued(last, queue)
    release_gate(next, queue)
    await_gate(last, queue)
    assert :ok = Task.await(pending, 1_000)
    assert {:ok, nil} = Cachex.get(Cache, {User, 3})

    release_gate(last, queue)
    assert Map.new(Cache.snapshot()) == %{{User, 1} => 13, {User, 2} => 47, {User, 3} => 79}
  end

  @tag capture_log: true
  test "a failed batch retries unsubmitted keys and newer pending values with its drain" do
    Cache.reset()
    on_exit(fn -> Cache.reset() end)
    queue = Process.whereis(name(Cache, :locksmith))
    collector = Process.whereis(Writer)
    initial = queue_gate(queue)
    await_gate(initial, queue)
    Cache.record({User, 1}, 77)
    queued_write(queue)
    keys = [{User, 2}, {User, 3}]
    Enum.each(keys, &Cache.record(&1, 12))
    drain = Task.async(&Writer.drain/0)
    await_drain(drain)
    next = queue_gate(queue)
    await_gate_queued(next, queue)
    release_gate(initial, queue)
    await_gate(next, queue)
    {batch, submitted} = queued_write(queue)
    [unsubmitted] = keys -- [submitted]
    Cache.record(submitted, 63)
    Cache.record(unsubmitted, 3)
    Cache.record({User, 4}, 19)
    pending_drain = Task.async(&Writer.drain/0)
    await_drain(pending_drain)
    ref = Process.monitor(batch)
    Process.exit(batch, :kill)
    assert_receive {:DOWN, ^ref, :process, ^batch, :killed}

    {retry, _key} = queued_write(queue, batch)
    refute retry == batch
    assert Task.yield(drain, 0) == nil
    assert Task.yield(pending_drain, 0) == nil
    release_gate(next, queue)
    assert :ok = Task.await(drain)
    assert :ok = Task.await(pending_drain)

    assert Map.new(Cache.snapshot()) ==
             %{{User, 1} => 77, submitted => 63, unsubmitted => 12, {User, 4} => 19}

    assert Process.whereis(Writer) == collector
  end

  @tag capture_log: true
  test "a writer restart stops its batch without losing completed Cachex values" do
    Cache.reset()
    Cache.record({User, 1}, 101)
    assert Cache.snapshot() == [{{User, 1}, 101}]
    queue = Process.whereis(name(Cache, :locksmith))

    :ok = Supervisor.terminate_child(Logflare.ContextCache.Supervisor, Writer)

    on_exit(fn ->
      {:ok, _writer} = Supervisor.restart_child(Logflare.ContextCache.Supervisor, Writer)
    end)

    writer = start_supervised!(Writer)

    on_exit(fn ->
      :sys.resume(queue)
      Cache.reset()
    end)

    :sys.suspend(queue)
    Cache.record({User, 2}, 12)

    batch =
      TestUtils.retry_assert(fn ->
        assert {:messages, [{:"$gen_call", {batch, _tag}, _request}]} =
                 Process.info(queue, :messages)

        batch
      end)

    ref = Process.monitor(batch)
    assert {:monitored_by, monitors} = Process.info(batch, :monitored_by)
    assert self() in monitors
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^batch, :killed}

    TestUtils.retry_assert(fn ->
      restarted = Process.whereis(Writer)
      assert is_pid(restarted)
      refute restarted == writer
    end)

    :sys.resume(queue)
    Cache.record({User, 2}, 19)
    assert Map.new(Cache.snapshot()) == %{{User, 1} => 101, {User, 2} => 19}
  end

  for finish <- [:commit, :rollback] do
    test "flush survives #{finish} of an in-progress user deletion" do
      Cache.reset()

      Sandbox.unboxed_run(Repo, fn ->
        [survivor, deleted] = insert_list(2, :user)

        on_exit(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            user_ids = [survivor.id, deleted.id]
            Repo.delete_all(from(u in User, where: u.id in ^user_ids))
          end)
        end)

        Cache.record({User, survivor.id}, 1_767_225_600)
        Cache.record({User, deleted.id}, 1_767_225_619)
        parent = self()

        flush =
          Task.async(fn ->
            Sandbox.unboxed_run(Repo, fn ->
              %{rows: [[pid]]} = Repo.query!("SELECT pg_backend_pid()")
              send(parent, {:flushing, pid})

              receive do
                :flush ->
                  try do
                    RecordUsages.flush()
                  rescue
                    error in Postgrex.Error -> {:error, error.postgres.code}
                  end
              end
            end)
          end)

        try do
          assert_receive {:flushing, flush_pid}, 1_000

          Repo.transaction(fn ->
            Repo.delete!(deleted)
            send(flush.pid, :flush)

            TestUtils.retry_assert(fn ->
              assert %{rows: [[true]]} =
                       Repo.query!(
                         "SELECT pg_backend_pid() = ANY(pg_blocking_pids($1::integer))",
                         [flush_pid]
                       )
            end)

            if unquote(finish) == :rollback, do: Repo.rollback(:control)
          end)

          assert Task.await(flush) == :ok
          assert Cache.snapshot() == []

          assert RecordUsages.get(User, survivor.id).last_used_at ==
                   ~U[2026-01-01 00:00:00Z]

          case unquote(finish) do
            :commit ->
              assert RecordUsages.get(User, deleted.id) == nil

            :rollback ->
              assert RecordUsages.get(User, deleted.id).last_used_at ==
                       ~U[2026-01-01 00:00:19Z]
          end
        after
          Task.shutdown(flush, :brutal_kill)
        end
      end)
    end
  end

  @spec queue_gate(pid()) :: {reference(), Task.t()}
  defp queue_gate(queue) do
    parent = self()
    ref = make_ref()
    on_exit(fn -> send(queue, {:release_gate, ref}) end)

    task =
      Task.async(fn ->
        Cachex.transaction(Cache, [], fn _cache ->
          send(parent, {:gate_held, ref, self()})

          receive do
            {:release_gate, ^ref} -> :ok
          after
            5_000 -> :safety_release
          end
        end)
      end)

    {ref, task}
  end

  @spec await_gate({reference(), Task.t()}, pid()) :: :ok
  defp await_gate({ref, _task}, queue) do
    assert_receive {:gate_held, ^ref, ^queue}, 2_000
    :ok
  end

  @spec await_gate_queued({reference(), Task.t()}, pid()) :: true
  defp await_gate_queued({_ref, %{pid: caller}}, queue) do
    TestUtils.retry_assert(fn ->
      {:messages, messages} = Process.info(queue, :messages)

      assert Enum.any?(
               messages,
               &match?({:"$gen_call", {^caller, _}, {:transaction, [], _, _}}, &1)
             )
    end)
  end

  @spec release_gate({reference(), Task.t()}, pid()) :: :ok
  defp release_gate({ref, task}, queue) do
    send(queue, {:release_gate, ref})
    assert {:ok, :ok} = Task.await(task)
    :ok
  end

  @spec queued_write(pid(), pid() | nil) :: {pid(), Cache.key()}
  defp queued_write(queue, excluded \\ nil) do
    TestUtils.retry_assert(fn ->
      {:messages, messages} = Process.info(queue, :messages)

      assert {pid, _key} =
               result =
               Enum.find_value(messages, fn
                 {:"$gen_call", {pid, _tag}, {:transaction, [key], _function, _options}}
                 when pid != excluded ->
                   {pid, key}

                 _ ->
                   nil
               end)

      assert is_pid(pid)
      result
    end)
  end

  @spec await_drain(Task.t()) :: true
  defp await_drain(task) do
    TestUtils.retry_assert(fn ->
      state = :sys.get_state(Writer)

      assert Enum.any?(state.waiters ++ state.pending_waiters, fn {pid, _tag} ->
               pid == task.pid
             end)
    end)
  end
end
