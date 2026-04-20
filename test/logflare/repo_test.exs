defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(hostnames) do
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, hostnames)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)
    start_supervised!({Replicas, hostnames: hostnames})
  end

  describe "apply_with_random_repo/3" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.apply_with_random_repo(Repo, :get_dynamic_repo, []) == Repo
    end

    test "uses replica repo during execution and reverts afterward" do
      start_read_replicas(["localhost", "127.0.0.1"])

      # since choice includes primary and replicas,
      # we may need to retry until we hit a replica
      retry_until_true(fn ->
        Repo.apply_with_random_repo(Repo, :get_dynamic_repo, []) != Repo
      end)

      # and then it's back to the default
      assert Repo.get_dynamic_repo() == Repo
    end

    test "reverts repo if function raises" do
      start_read_replicas(["localhost"])

      assert_raise ArithmeticError, fn ->
        Repo.apply_with_random_repo(Kernel, :/, [1, 0])
      end

      # and then it's back to the default
      assert Repo.get_dynamic_repo() == Repo
    end
  end

  defp retry_until_true(fun) do
    case fun.() do
      true -> :ok
      false -> retry_until_true(fun)
    end
  end
end
