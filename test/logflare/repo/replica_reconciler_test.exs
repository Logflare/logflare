defmodule Logflare.Repo.ReplicaReconcilerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Repo.ReplicaReconciler
  alias Logflare.Repo.Replicas

  @check_opts [lag: :timer.minutes(1), recheck_delay: 0]

  setup do
    on_exit(fn -> :persistent_term.erase({ReplicaReconciler, :disabled}) end)
    :ok
  end

  defp configure_replicas(entries) do
    previous = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, previous) end)
  end

  defp start_unsandboxed_replica do
    {key, _config} = entry = Replicas.parse!("127.0.0.1")
    start_supervised!({Replicas, entries: [entry]})
    configure_replicas([entry])
    key
  end

  defp insert_stale_source do
    user = insert(:user)
    stale = NaiveDateTime.add(NaiveDateTime.utc_now(), -1, :hour)
    insert(:source, user: user, updated_at: stale, inserted_at: stale)
  end

  describe "reconcile/1" do
    test "keeps an in-sync replica enabled" do
      key = start_unsandboxed_replica()

      assert %{^key => :ok} = ReplicaReconciler.reconcile(@check_opts)
      refute ReplicaReconciler.disabled?(key)
      refute Repo.with_replica(&Repo.get_dynamic_repo/0) == Repo
    end

    test "ignores writes inside the replication lag window" do
      key = start_unsandboxed_replica()
      insert(:source, user: insert(:user))

      assert %{^key => :ok} = ReplicaReconciler.reconcile(@check_opts)
      refute ReplicaReconciler.disabled?(key)
    end

    test "disables an out-of-sync replica and re-enables it once it passes again" do
      key = start_unsandboxed_replica()
      source = insert_stale_source()

      {result, log} = with_log(fn -> ReplicaReconciler.reconcile(@check_opts) end)

      assert %{
               ^key =>
                 {:mismatch, [%{table: "sources", primary: {p_count, _}, replica: {r_count, _}}]}
             } =
               result

      assert p_count == r_count + 1
      assert log =~ "re-checking"
      assert log =~ "Read replica out of sync with primary"
      assert ReplicaReconciler.disabled?(key)

      ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :replica_route]])
      on_exit(fn -> :telemetry.detach(ref) end)

      assert Repo.with_replica(&Repo.get_dynamic_repo/0) == Repo

      assert_receive {[:logflare, :repo, :replica_route], ^ref, _,
                      %{role: :primary, reason: :disabled}}

      Repo.delete!(source)

      assert %{^key => :ok} = ReplicaReconciler.reconcile(@check_opts)
      refute ReplicaReconciler.disabled?(key)
      refute Repo.with_replica(&Repo.get_dynamic_repo/0) == Repo
    end

    test "only disables the replica that is out of sync" do
      {key, _config} = entry = Replicas.parse!("127.0.0.1")
      start_supervised!({Replicas, entries: [entry]})
      configure_replicas([entry, Repo])
      insert_stale_source()

      capture_log(fn ->
        assert %{^key => {:mismatch, _}, Repo => :ok} = ReplicaReconciler.reconcile(@check_opts)
      end)

      assert ReplicaReconciler.disabled_replicas() == MapSet.new([key])
      assert Enum.all?(1..20, fn _ -> Repo.with_replica(&Repo.get_dynamic_repo/0) == Repo end)
    end

    test "detects an out-of-sync users table" do
      key = start_unsandboxed_replica()
      stale = NaiveDateTime.add(NaiveDateTime.utc_now(), -1, :hour)
      insert(:user, updated_at: stale, inserted_at: stale)

      capture_log(fn ->
        assert %{^key => {:mismatch, [%{table: "users"}]}} =
                 ReplicaReconciler.reconcile(@check_opts)
      end)

      assert ReplicaReconciler.disabled?(key)
    end
  end

  describe "GenServer" do
    test "runs as a no-op when no replicas are configured" do
      configure_replicas([])

      pid = start_supervised!({ReplicaReconciler, name: nil, initial_delay: 0, jitter: 0})
      :sys.get_state(pid)

      assert Process.alive?(pid)
      assert ReplicaReconciler.reconcile(@check_opts) == %{}
      assert ReplicaReconciler.disabled_replicas() == MapSet.new()
    end

    test "disables an out-of-sync replica on a scheduled run" do
      key = start_unsandboxed_replica()
      insert_stale_source()

      pid =
        start_supervised!(
          {ReplicaReconciler,
           name: nil, initial_delay: :timer.hours(1), jitter: 0, recheck_delay: 0}
        )

      capture_log(fn ->
        send(pid, :reconcile)
        :sys.get_state(pid)
      end)

      assert ReplicaReconciler.disabled?(key)
    end
  end
end
