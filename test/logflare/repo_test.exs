defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(hostnames) do
    primary_hostname = Keyword.fetch!(Repo.config(), :hostname)

    # sanity check that our test replicas are not the same as the primary
    for hostname <- hostnames do
      refute hostname == primary_hostname,
             "replica hostname #{hostname} should be different from primary hostname"
    end

    # we read the replicas from env in `apply_with_random_repo/3`, so we need to set it there for the test
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, hostnames)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)

    # attach repo init handler to ensure we start the replicas with the expected config
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    start_result = start_supervised!({Replicas, hostnames: hostnames})

    for hostname <- hostnames do
      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      assert Keyword.fetch!(opts, :hostname) == hostname
    end

    start_result
  end

  describe "apply_with_random_repo/3" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.apply_with_random_repo(Repo, :get_dynamic_repo, []) == Repo
    end

    test "uses replica repo during execution and reverts afterward" do
      start_read_replicas(["127.0.0.1", "::1"])

      # since random repo choice includes body primary and replicas,
      # we may need to retry our check until we hit a replica
      repos = for _ <- 1..30, do: Repo.apply_with_random_repo(Repo, :get_dynamic_repo, [])

      assert Enum.any?(repos, fn repo -> repo != Repo end),
             "expected at least one call to use a replica"

      # verify that after the call, we're back to the default repo
      assert Repo.get_dynamic_repo() == Repo
    end

    test "reverts repo if function raises" do
      start_read_replicas(["127.0.0.1"])

      assert_raise ArithmeticError, fn ->
        Repo.apply_with_random_repo(Kernel, :/, [1, 0])
      end

      assert Repo.get_dynamic_repo() == Repo
    end
  end
end
