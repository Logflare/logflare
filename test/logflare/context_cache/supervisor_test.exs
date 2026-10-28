defmodule Logflare.ContextCache.SupervisorTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Cainophile.Adapters.Postgres
  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.Supervisor
  alias Logflare.Repo.ReplicaReconciler
  alias Logflare.Repo.Replicas

  setup do
    on_exit(fn ->
      :persistent_term.erase({ReplicaReconciler, :disabled})
      :persistent_term.erase({Supervisor, :wal_replica})
    end)

    :ok
  end

  defp put_env(key, value) do
    previous = Application.get_env(:logflare, key)
    Application.put_env(:logflare, key, value)
    on_exit(fn -> Application.put_env(:logflare, key, previous) end)
  end

  defp prefer_read_replica(value) do
    put_env(
      CacheBuster,
      Keyword.put(Application.get_env(:logflare, CacheBuster), :prefer_read_replica, value)
    )
  end

  defp start_unsandboxed_replica do
    {key, _config} = entry = Replicas.parse!("127.0.0.1")
    start_supervised!({Replicas, entries: [entry]})
    {key, entry}
  end

  defp insert_stale_source do
    stale = NaiveDateTime.add(NaiveDateTime.utc_now(), -1, :hour)
    insert(:source, user: insert(:user), updated_at: stale, inserted_at: stale)
  end

  defp epgsql_host({_replica, opts}), do: Keyword.fetch!(opts, :epgsql).host

  describe "cainophile_options/0" do
    test "follows the primary by default" do
      {_key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry])
      prefer_read_replica(false)

      assert {nil, opts} = Supervisor.cainophile_options()
      assert Keyword.fetch!(opts, :epgsql).host == String.to_charlist(Repo.config()[:hostname])
    end

    test "follows a replica that passes reconciliation" do
      {key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry])
      prefer_read_replica(true)

      assert {^key, _opts} = options = Supervisor.cainophile_options()
      assert epgsql_host(options) == ~c"127.0.0.1"
    end

    test "skips a replica that fails reconciliation" do
      {_key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry, Repo])
      prefer_read_replica(true)
      insert_stale_source()

      {options, log} = with_log(fn -> Supervisor.cainophile_options() end)

      assert {Repo, _opts} = options
      assert log =~ "not eligible to follow WAL"
    end

    test "skips a replica disabled by the reconciler" do
      {key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry])
      prefer_read_replica(true)
      source = insert_stale_source()

      capture_log(fn -> ReplicaReconciler.reconcile() end)
      assert ReplicaReconciler.disabled?(key)
      Repo.delete!(source)

      capture_log(fn -> assert {nil, _opts} = Supervisor.cainophile_options() end)
    end
  end

  describe "start_cainophile/0" do
    setup do
      test_pid = self()
      running = GenServer.whereis(Supervisor.publisher_name())
      :syn.unregister(:core, Logflare.PgPublisher)

      on_exit(fn ->
        :syn.unregister(:core, Logflare.PgPublisher)
        if running, do: :syn.register(:core, Logflare.PgPublisher, running)
      end)

      stub(Postgres, :start_link, fn opts ->
        pid = spawn(fn -> Process.sleep(:infinity) end)
        :syn.unregister(:core, Logflare.PgPublisher)
        :ok = :syn.register(:core, Logflare.PgPublisher, pid)
        send(test_pid, {:cainophile_started, pid, Keyword.fetch!(opts, :epgsql).host})
        {:ok, pid}
      end)

      :ok
    end

    test "restarts on another source when the followed replica is disabled" do
      {key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry])
      prefer_read_replica(true)

      assert {:ok, pid} = Supervisor.start_cainophile()
      assert_receive {:cainophile_started, ^pid, ~c"127.0.0.1"}
      assert Supervisor.wal_replica() == key
      ref = Process.monitor(pid)

      insert_stale_source()
      capture_log(fn -> ReplicaReconciler.reconcile() end)

      assert_receive {:DOWN, ^ref, :process, ^pid, :replica_disabled}

      capture_log(fn -> assert {:ok, _restarted} = Supervisor.start_cainophile() end)
      assert_receive {:cainophile_started, _restarted, host}
      assert host == String.to_charlist(Repo.config()[:hostname])
      assert Supervisor.wal_replica() == nil
    end

    test "keeps following the primary when a replica is disabled" do
      {_key, entry} = start_unsandboxed_replica()
      put_env(:read_replicas, [entry])
      prefer_read_replica(false)

      assert {:ok, pid} = Supervisor.start_cainophile()
      on_exit(fn -> Process.exit(pid, :kill) end)
      ref = Process.monitor(pid)

      insert_stale_source()
      capture_log(fn -> ReplicaReconciler.reconcile() end)

      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}, 100
    end
  end
end
