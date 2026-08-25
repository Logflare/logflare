defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(raw_entries) do
    primary_hostname = Keyword.fetch!(Repo.config(), :hostname)
    entries = Enum.map(raw_entries, &Replicas.parse!/1)

    # sanity check that our test replicas are not the same as the primary
    for {_key, config} <- entries do
      refute config[:hostname] == primary_hostname,
             "replica hostname #{config[:hostname]} should be different from primary hostname"
    end

    # we read the replicas from env in `apply_with_replica/3`, so we need to set it there for the test
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)

    # attach repo init handler to ensure we start the replicas with the expected config
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    start_result = start_supervised!({Replicas, entries: entries})

    for {_key, config} <- entries do
      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}

      for {k, v} <- config, k != :ssl do
        assert Keyword.fetch!(opts, k) == v
      end
    end

    start_result
  end

  describe "apply_with_primary/3" do
    test "uses the primary repo during execution and restores the previous repo" do
      previous_repo = Repo.get_dynamic_repo()
      fake_replica = Module.concat(__MODULE__, FakeReplica)
      Repo.put_dynamic_repo(fake_replica)

      try do
        assert Repo.apply_with_primary(Repo, :get_dynamic_repo, []) == Repo
        assert Repo.get_dynamic_repo() == fake_replica
      after
        Repo.put_dynamic_repo(previous_repo)
      end
    end
  end

  describe "apply_with_replica/3" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.apply_with_replica(Repo, :get_dynamic_repo, []) == Repo
    end

    test "always uses a replica when replicas are configured" do
      start_read_replicas(["127.0.0.1", "::1"])

      repos = for _ <- 1..30, do: Repo.apply_with_replica(Repo, :get_dynamic_repo, [])

      refute Enum.any?(repos, fn repo -> repo == Repo end),
             "expected every call to use a replica, never the primary"

      # verify that after the call, we're back to the default repo
      assert Repo.get_dynamic_repo() == Repo
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
  end
end
