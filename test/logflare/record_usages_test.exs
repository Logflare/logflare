defmodule Logflare.RecordUsagesTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import ExUnit.CaptureLog
  import Logflare.Factory

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.Writer
  alias Logflare.Repo
  alias Logflare.TestUtils
  alias Logflare.User

  test "recordings bound the mailbox without waiting for the writer" do
    Cache.reset()
    writer = Process.whereis(Writer)

    on_exit(fn ->
      :sys.resume(writer)
      Cache.reset()
    end)

    :sys.suspend(writer)

    for timestamp <- 1..1_050 do
      assert :ok = Writer.record({User, 1}, timestamp)
    end

    assert Process.info(writer, :message_queue_len) == {:message_queue_len, 1_000}
    :sys.resume(writer)
    assert Enum.to_list(Cache.stream()) == [{{User, 1}, 1_000}]

    Writer.record({User, 1}, 1_051)
    assert Enum.to_list(Cache.stream()) == [{{User, 1}, 1_051}]
  end

  test "failed writes are logged and discarded without stopping later recordings" do
    Cache.reset()
    writer = Process.whereis(Writer)

    on_exit(fn ->
      if Process.whereis(Cache) == nil do
        Supervisor.restart_child(Logflare.ContextCache.Supervisor, Cache)
      end

      Cache.reset()
    end)

    assert :ok = Supervisor.terminate_child(Logflare.ContextCache.Supervisor, Cache)

    log =
      capture_log([format: "$level $message $metadata", metadata: [:error_string]], fn ->
        assert :ok = Writer.record({User, 1}, 109)
        assert :ok = Writer.drain()
      end)

    assert log =~ "warning Record usage cache write failed"
    assert log =~ "no_cache"
    assert Process.whereis(Writer) == writer
    assert {:ok, _cache} = Supervisor.restart_child(Logflare.ContextCache.Supervisor, Cache)
    assert Enum.to_list(Cache.stream(drain: false)) == []
    assert :ok = Writer.record({User, 1}, 103)
    assert :ok = Writer.record({User, 2}, 47)
    assert Map.new(Cache.stream()) == %{{User, 1} => 103, {User, 2} => 47}
  end

  test "recordings preserve newer and equal cached timestamps while advancing older and missing ones" do
    Cache.reset()
    on_exit(fn -> Cache.reset() end)
    assert Cachex.put_many!(Cache, [{{User, 1}, 101}, {{User, 2}, 103}, {{User, 3}, 107}])
    Writer.record({User, 1}, 99)
    Writer.record({User, 2}, 103)
    Writer.record({User, 3}, 109)
    Writer.record({User, 4}, 113)

    expected = %{{User, 1} => 101, {User, 2} => 103, {User, 3} => 109, {User, 4} => 113}
    assert Map.new(Cache.stream()) == expected

    assert :ok = Cache.acknowledge([{{User, 3}, 107}])
    assert Map.new(Cache.stream(drain: false)) == expected
    assert :ok = Cache.acknowledge([{{User, 3}, 109}])
    assert Map.new(Cache.stream(drain: false)) == Map.delete(expected, {User, 3})
  end

  describe "with cache writes blocked" do
    setup do
      Cache.reset()
      parent = self()

      transaction =
        Task.async(fn ->
          Cachex.transaction(Cache, [], fn _cache ->
            send(parent, {:transaction_held, self()})

            receive do
              :release -> :ok
            after
              5_000 -> :timeout
            end
          end)
        end)

      assert_receive {:transaction_held, queue}, 1_000

      on_exit(fn ->
        if Process.alive?(transaction.pid), do: send(queue, :release)
        Cache.reset()
      end)

      {:ok, transaction: transaction, queue: queue}
    end

    test "equal and older recordings skip the transaction queue", %{
      queue: queue,
      transaction: held
    } do
      key = {User, 1}
      assert Cachex.put!(Cache, key, 103)
      Writer.record(key, 103)
      Writer.record(key, 101)
      drain = Task.async(&Writer.drain/0)

      assert Task.await(drain, 1_000) == :ok
      assert Enum.to_list(Cache.stream(drain: false)) == [{key, 103}]
      send(queue, :release)
      assert Task.await(held) == {:ok, :ok}
    end

    test "drain waits for preceding recordings", %{queue: queue, transaction: held} do
      drain =
        Task.async(fn ->
          Writer.record({User, 1}, 13)
          Writer.drain()
        end)

      refute Task.yield(drain, 50)
      send(queue, :release)
      assert Task.await(held) == {:ok, :ok}
      assert Task.await(drain) == :ok
      assert Enum.to_list(Cache.stream(drain: false)) == [{{User, 1}, 13}]
    end
  end

  @tag capture_log: true
  test "a writer restart preserves completed Cachex values" do
    Cache.reset()
    on_exit(fn -> Cache.reset() end)
    Writer.record({User, 1}, 101)
    assert Enum.to_list(Cache.stream()) == [{{User, 1}, 101}]
    writer = Process.whereis(Writer)
    Process.exit(writer, :kill)

    TestUtils.retry_assert(fn ->
      restarted = Process.whereis(Writer)
      assert is_pid(restarted)
      refute restarted == writer
    end)

    Writer.record({User, 2}, 19)
    assert Map.new(Cache.stream()) == %{{User, 1} => 101, {User, 2} => 19}
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

        Writer.record({User, survivor.id}, 1_767_225_600)
        Writer.record({User, deleted.id}, 1_767_225_619)
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
          assert Cache.stream() |> Enum.to_list() == []

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
end
