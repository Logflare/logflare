defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.QueryContext
  alias Logflare.Repo
  alias Logflare.Repo.Replicas
  alias Logflare.Telemetry

  defp start_read_replicas(raw_entries, entry_overrides \\ []) do
    primary_hostname = Keyword.fetch!(Repo.config(), :hostname)

    entries =
      Enum.map(raw_entries, fn entry ->
        {key, config} = Replicas.parse!(entry)
        {key, Keyword.merge(config, entry_overrides)}
      end)

    for {_key, config} <- entries do
      refute config[:hostname] == primary_hostname,
             "replica hostname #{config[:hostname]} should be different from primary hostname"
    end

    # apply_with_replica/3 reads the configured entries from the application environment.
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)

    # Observe the effective Ecto configuration for each replica pool.
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    start_result = start_supervised!({Replicas, entries: entries})

    for {_key, config} <- entries do
      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}

      for {k, v} <- config,
          k not in [:ssl, :logflare_auth, :logflare_aws_region] do
        assert Keyword.fetch!(opts, k) == v
      end

      refute Keyword.has_key?(opts, :logflare_connection_role)
      refute Keyword.has_key?(opts, :logflare_auth)
      refute Keyword.has_key?(opts, :logflare_aws_region)

      assert {Replicas, :after_connect, [_primary_after_connect]} =
               Keyword.fetch!(opts, :after_connect)
    end

    start_result
  end

  describe "after_commit/1" do
    setup do
      owner = Sandbox.start_owner!(Repo, shared: true)
      on_exit(fn -> Sandbox.stop_owner(owner) end)
      :ok
    end

    test "runs immediately outside a transaction and in order after nested commits" do
      Repo.after_commit(fn -> send(self(), :immediate) end)
      assert_received :immediate

      assert {:ok, :done} =
               Repo.transaction(fn ->
                 Repo.after_commit(fn -> send(self(), {:committed, :first}) end)

                 assert {:ok, :ok} =
                          Repo.transact(fn ->
                            Repo.after_commit(fn -> send(self(), {:committed, :second}) end)
                            {:ok, :ok}
                          end)

                 refute_received {:committed, _}
                 :done
               end)

      assert_receive {:committed, first}
      assert first == :first
      assert_receive {:committed, second}
      assert second == :second
    end

    test "callback failures preserve the commit result and do not skip later callbacks" do
      log =
        capture_log(fn ->
          assert {:ok, :committed} =
                   Repo.transact(fn ->
                     Repo.after_commit(fn -> raise "cache restarting" end)
                     Repo.after_commit(fn -> exit(:cache_restarting) end)
                     Repo.after_commit(fn -> throw(:cache_restarting) end)
                     Repo.after_commit(fn -> send(self(), :still_committed) end)
                     {:ok, :committed}
                   end)
        end)

      assert_received :still_committed
      assert log =~ "After-commit callback failed"
      assert log =~ "cache restarting"
      assert log =~ "cache_restarting"
      assert {:ok, :next} = Repo.transact(fn -> {:ok, :next} end)
    end

    test "immediate callbacks isolate failures without hiding the successful write" do
      log =
        capture_log(fn ->
          assert :ok = Repo.after_commit(fn -> raise "cache restarting" end)
          assert :ok = Repo.after_commit(fn -> send(self(), :next_callback) end)
        end)

      assert_received :next_callback
      assert log =~ "After-commit callback failed"
    end

    test "rollback and exceptions discard callbacks without leaking into later transactions" do
      assert {:error, :cancelled} =
               Repo.transaction(fn ->
                 Repo.after_commit(fn -> send(self(), :stale) end)
                 Repo.rollback(:cancelled)
               end)

      assert_raise ArgumentError, "cancelled", fn ->
        Repo.transact(fn ->
          Repo.after_commit(fn -> send(self(), :stale) end)
          raise ArgumentError, "cancelled"
        end)
      end

      assert {:ok, :ok} = Repo.transact(fn -> {:ok, :ok} end)
      refute_received :stale
    end

    test "Ecto.Multi callbacks run only after a successful commit" do
      multi =
        Ecto.Multi.new()
        |> Ecto.Multi.run(:change, fn _, _ ->
          Repo.after_commit(fn -> send(self(), :multi_committed) end)
          refute_received :multi_committed
          {:ok, :done}
        end)

      assert {:ok, %{change: :done}} = Repo.transact(multi)
      assert_receive :multi_committed

      failed =
        Ecto.Multi.new()
        |> Ecto.Multi.run(:change, fn _, _ ->
          Repo.after_commit(fn -> send(self(), :multi_rolled_back) end)
          {:error, :cancelled}
        end)

      assert {:error, :change, :cancelled, %{}} = Repo.transact(failed)
      refute_received :multi_rolled_back
    end
  end

  test "query-context cache fills read primary even inside a replica scope" do
    start_read_replicas(["127.0.0.1"], pool: DBConnection.ConnectionPool, pool_size: 1)
    owner = Sandbox.start_owner!(Repo, shared: true)
    on_exit(fn -> Sandbox.stop_owner(owner) end)
    Cachex.clear!(QueryContext.Cache)
    on_exit(fn -> Cachex.clear!(QueryContext.Cache) end)
    user = Logflare.Factory.insert(:user)
    backend = Logflare.Factory.insert(:backend, user: user)

    Repo.with_replica(fn ->
      replica = Repo.get_dynamic_repo()
      assert Repo.query!("SELECT id FROM backends WHERE id = $1", [backend.id]).rows == []
      assert QueryContext.get_user(user.id).id == user.id
      assert QueryContext.get_backend(backend.id).id == backend.id
      assert [cached] = QueryContext.list_backends(user.id)
      assert cached.id == backend.id
      assert Repo.get_dynamic_repo() == replica
    end)
  end

  test "query metrics distinguish primary and replica pools using the same repo module" do
    start_read_replicas(["127.0.0.1"], pool: DBConnection.ConnectionPool, pool_size: 1)
    owner = Sandbox.start_owner!(Repo, shared: true)
    on_exit(fn -> Sandbox.stop_owner(owner) end)

    metrics =
      Enum.filter(Telemetry.metrics(), &(&1.event_name == [:logflare, :repo, :query]))

    assert length(metrics) == 4
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:logflare, :repo, :query],
        fn _, _, metadata, _ ->
          if metadata.query == "SELECT 1" do
            for metric <- metrics do
              send(test_pid, {:query_role, metric.name, metric.tag_values.(metadata)})
            end
          end
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    for role <- [:primary, :replica] do
      case role do
        :primary -> Repo.query!("SELECT 1", [])
        :replica -> Repo.with_replica(fn -> Repo.query!("SELECT 1", []) end)
      end

      for metric <- metrics do
        assert metric.tags == [:role]
        name = metric.name
        assert_receive {:query_role, ^name, %{role: ^role}}
      end
    end
  end

  describe "with_replica/1" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.with_replica(&Repo.get_dynamic_repo/0) == Repo
    end

    test "always uses a replica when replicas are configured" do
      start_read_replicas(["127.0.0.1", "::1"])

      repos = for _ <- 1..30, do: Repo.with_replica(&Repo.get_dynamic_repo/0)

      refute Enum.any?(repos, fn repo -> repo == Repo end),
             "expected every call to use a replica, never the primary"

      assert Repo.get_dynamic_repo() == Repo
    end

    test "keeps nested reads on the selected replica" do
      start_read_replicas(["127.0.0.1", "::1"])

      {outer_repo, inner_repo} =
        Repo.with_replica(fn ->
          {Repo.get_dynamic_repo(), Repo.with_replica(&Repo.get_dynamic_repo/0)}
        end)

      assert outer_repo == inner_repo
      refute outer_repo == Repo
      assert Repo.get_dynamic_repo() == Repo
    end

    test "keeps reads on the primary inside an existing transaction" do
      start_read_replicas(["127.0.0.1"])
      owner = Sandbox.start_owner!(Repo, shared: true)
      on_exit(fn -> Sandbox.stop_owner(owner) end)

      assert {:ok, Repo} = Repo.transaction(fn -> Repo.with_replica(&Repo.get_dynamic_repo/0) end)
    end

    test "emits the selected route" do
      start_read_replicas(["127.0.0.1"])
      ref = :telemetry_test.attach_event_handlers(self(), [[:logflare, :repo, :replica_route]])
      on_exit(fn -> :telemetry.detach(ref) end)

      Repo.with_replica(&Repo.get_dynamic_repo/0)

      assert_receive {[:logflare, :repo, :replica_route], ^ref, %{count: 1},
                      %{role: :replica, reason: :selected}}
    end
  end

  describe "with_primary/1" do
    test "reads on the primary inside replica scope and restores that scope" do
      start_read_replicas(["127.0.0.1"])

      Repo.with_replica(fn ->
        replica = Repo.get_dynamic_repo()
        refute replica == Repo
        assert Repo.with_primary(&Repo.get_dynamic_repo/0) == Repo
        assert Repo.get_dynamic_repo() == replica
        assert_raise ArithmeticError, fn -> Repo.with_primary(fn -> raise ArithmeticError end) end
        assert Repo.get_dynamic_repo() == replica
      end)

      assert Repo.get_dynamic_repo() == Repo
    end

    test "preserves an existing primary transaction" do
      owner = Sandbox.start_owner!(Repo, shared: true)
      on_exit(fn -> Sandbox.stop_owner(owner) end)

      assert {:ok, true} = Repo.transaction(fn -> Repo.with_primary(&Repo.in_transaction?/0) end)
    end
  end

  describe "with_read/2" do
    test "false selects primary even when replicas are configured" do
      start_read_replicas(["127.0.0.1"])

      assert Repo.with_read(fn -> {:primary, Repo.get_dynamic_repo()} end, false) ==
               {:primary, Repo}

      assert Repo.get_dynamic_repo() == Repo
    end

    test "true selects a replica and restores the caller's repo" do
      start_read_replicas(["127.0.0.1"])

      refute Repo.with_read(&Repo.get_dynamic_repo/0, true) == Repo
      assert Repo.get_dynamic_repo() == Repo
    end

    test "true falls back to primary when no replicas are configured" do
      start_read_replicas([])

      assert Repo.with_read(&Repo.get_dynamic_repo/0, true) == Repo
      assert Repo.get_dynamic_repo() == Repo
    end

    test "nested reads retain the replica while false forces primary and restores scope" do
      start_read_replicas(["127.0.0.1"])

      Repo.with_read(
        fn ->
          replica = Repo.get_dynamic_repo()
          refute replica == Repo
          assert Repo.with_read(&Repo.get_dynamic_repo/0, true) == replica
          assert Repo.with_read(&Repo.get_dynamic_repo/0, false) == Repo
          assert Repo.get_dynamic_repo() == replica

          assert_raise ArithmeticError, fn ->
            Repo.with_read(fn -> raise ArithmeticError end, false)
          end

          assert Repo.get_dynamic_repo() == replica
        end,
        true
      )

      assert Repo.get_dynamic_repo() == Repo
    end

    test "true restores the caller's repo when the read raises" do
      start_read_replicas(["127.0.0.1"])

      assert_raise ArithmeticError, fn ->
        Repo.with_read(fn -> raise ArithmeticError end, true)
      end

      assert Repo.get_dynamic_repo() == Repo
    end

    for replica? <- [true, false] do
      @use_replica replica?

      test "#{replica?} preserves an existing primary transaction" do
        start_read_replicas(["127.0.0.1"])
        owner = Sandbox.start_owner!(Repo, shared: true)
        on_exit(fn -> Sandbox.stop_owner(owner) end)

        assert {:ok, {Repo, true}} =
                 Repo.transaction(fn ->
                   Repo.with_read(
                     fn -> {Repo.get_dynamic_repo(), Repo.in_transaction?()} end,
                     @use_replica
                   )
                 end)
      end
    end

    test "requires an explicit boolean policy" do
      for policy <- [nil, :replica, "true"] do
        assert_raise FunctionClauseError, fn -> Repo.with_read(fn -> :ok end, policy) end
      end
    end
  end

  describe "apply_with_replica/3" do
    test "delegates to scoped replica routing" do
      start_read_replicas(["127.0.0.1"])

      refute Repo.apply_with_replica(Repo, :get_dynamic_repo, []) == Repo
      assert Repo.get_dynamic_repo() == Repo
    end

    test "runs the primary hook before making replica pool connections read-only" do
      previous_repo_config = Application.fetch_env!(:logflare, Repo)

      Application.put_env(
        :logflare,
        Repo,
        Keyword.put(
          previous_repo_config,
          :after_connect,
          {Postgrex, :query!, ["SET application_name = 'replica_after_connect'", []]}
        )
      )

      on_exit(fn -> Application.put_env(:logflare, Repo, previous_repo_config) end)

      start_read_replicas(["127.0.0.1"],
        pool: DBConnection.ConnectionPool,
        pool_size: 1
      )

      assert %Postgrex.Result{rows: [["replica_after_connect"]]} =
               Repo.apply_with_replica(Repo, :query!, ["SHOW application_name", []])

      assert %Postgrex.Result{rows: [["on"]]} =
               Repo.apply_with_replica(
                 Repo,
                 :query!,
                 ["SHOW default_transaction_read_only", []]
               )

      assert_raise Postgrex.Error, ~r/read-only transaction/, fn ->
        Repo.apply_with_replica(
          Repo,
          :query!,
          ["CREATE TEMP TABLE read_only_replica_probe (id integer)", []]
        )
      end
    end

    test "reverts repo if function raises" do
      start_read_replicas(["127.0.0.1"])

      assert_raise ArithmeticError, fn ->
        Repo.apply_with_replica(Kernel, :/, [1, 0])
      end

      assert Repo.get_dynamic_repo() == Repo
    end

    test "supports declaring a replica as a URI alongside a bare hostname" do
      start_read_replicas(["127.0.0.1", "postgres://127.0.0.2:5433/replica_db"])

      repos = for _ <- 1..30, do: Repo.apply_with_replica(Repo, :get_dynamic_repo, [])
      assert Enum.all?(repos, &(&1 != Repo))
    end
  end

  describe "Replicas.after_connect/2" do
    setup do
      opts = Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])

      %{conn: start_supervised!({Postgrex, opts})}
    end

    test "rejects writes on the session", %{conn: conn} do
      Replicas.after_connect(conn, _no_primary_hook = nil)

      assert_raise Postgrex.Error, ~r/read-only transaction/, fn ->
        Postgrex.query!(conn, "CREATE TEMP TABLE read_only_probe (id integer)", [])
      end
    end

    test "still runs the primary's after_connect, given as an MFA", %{conn: conn} do
      Replicas.after_connect(conn, {Postgrex, :query!, ["SET application_name = 'mfa'", []]})

      assert %Postgrex.Result{rows: [["mfa"]]} =
               Postgrex.query!(conn, "SHOW application_name", [])

      assert %Postgrex.Result{rows: [["on"]]} =
               Postgrex.query!(conn, "SHOW default_transaction_read_only", [])
    end

    test "runs a function after_connect before making the session read-only", %{conn: conn} do
      test_pid = self()

      hook = fn conn ->
        %Postgrex.Result{rows: [[setting]]} =
          Postgrex.query!(conn, "SHOW default_transaction_read_only", [])

        send(test_pid, {:read_only_setting_in_primary_hook, setting})
      end

      Replicas.after_connect(conn, hook)

      assert_receive {:read_only_setting_in_primary_hook, "off"}

      assert %Postgrex.Result{rows: [["on"]]} =
               Postgrex.query!(conn, "SHOW default_transaction_read_only", [])
    end
  end

  describe "Replicas.parse/1" do
    test "a bare hostname only overrides the hostname, keyed by hostname" do
      assert {:ok, {"host", [hostname: "host"]}} = Replicas.parse("host")
    end

    test "a URI only overrides the parts given, keyed without credentials or primary info" do
      cases = [
        {"postgres://u:pass@host:5433/db",
         [hostname: "host", port: 5433, database: "db", username: "u", password: "pass"], "host"},
        {"postgresql://host", [hostname: "host"], "host"},
        {"postgres://host?ssl=true&pool_size=5", [hostname: "host", ssl: true, pool_size: 5],
         "host"},
        {"postgres://u:p%40ss@host/my%20db",
         [hostname: "host", database: "my db", username: "u", password: "p@ss"], "host"}
      ]

      for {entry, expected_config, expected_key_prefix} <- cases do
        assert {:ok, {key, config}} = Replicas.parse(entry)

        assert key =~ "#{expected_key_prefix}-"
        refute key =~ "pass"
        refute key =~ "p@ss"
        refute key =~ "p%40ss"

        for {k, v} <- expected_config do
          assert Keyword.fetch!(config, k) == v, "mismatch on #{k} for #{entry}"
        end
      end
    end

    test "two URIs with the same host/port/database but different credentials get distinct keys" do
      assert {:ok, {key1, _}} = Replicas.parse("postgres://a:1@host:5432/db")
      assert {:ok, {key2, _}} = Replicas.parse("postgres://b:2@host:5432/db")
      refute key1 == key2
    end

    test "an omitted host inherits the primary's, without baking it into the parsed config" do
      assert {:ok, {key, config}} = Replicas.parse("postgres:///db")
      refute Keyword.has_key?(config, :hostname)
      assert key =~ ~r{^-\d+$}
    end

    test "rejects invalid entries" do
      for entry <- [
            "postgres://host?pool_size=abc"
          ] do
        assert {:error, _reason} = Replicas.parse(entry), "expected #{entry} to be rejected"
      end
    end

    test "parse!/1 raises without leaking credentials" do
      error =
        assert_raise ArgumentError, fn ->
          Replicas.parse!("postgres://u:supersecret@host?pool_size=abc")
        end

      refute Exception.message(error) =~ "supersecret"
    end

    test "parse!/1 redacts credentials in auth validation errors" do
      error =
        assert_raise ArgumentError, fn ->
          Replicas.parse!("postgres://u:supersecret@host?auth=kerberos")
        end

      refute Exception.message(error) =~ "supersecret"
    end
  end
end
