defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(urls) do
    initial = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, urls)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, initial) end)

    start_supervised!({Replicas, urls: urls})
  end

  describe "apply_with_read_replica/3" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.apply_with_read_replica(Repo, :get_dynamic_repo, []) == Repo
    end

    test "uses replica repo during execution and reverts afterward" do
      %{
        username: username,
        password: password,
        hostname: hostname,
        port: port,
        database: database
      } = Map.new(Repo.config())

      start_read_replicas([
        "ecto://#{username}:#{password}@#{hostname}:#{port}/#{database}?pool_size=1",
        "postgres://#{username}:#{password}@#{hostname}:#{port}/#{database}?pool_size=1"
      ])

      replica = Repo.apply_with_read_replica(Repo, :get_dynamic_repo, [])
      assert is_pid(replica)

      # and then it's back to the default
      assert Repo.get_dynamic_repo() == Repo
    end

    test "reverts repo if function raises" do
      %{
        username: username,
        password: password,
        hostname: hostname,
        port: port,
        database: database
      } = Map.new(Repo.config())

      start_read_replicas([
        "ecto://#{username}:#{password}@#{hostname}:#{port}/#{database}?pool_size=1"
      ])

      assert_raise ArithmeticError, fn ->
        Repo.apply_with_read_replica(Kernel, :/, [1, 0])
      end

      # still back to the default
      assert Repo.get_dynamic_repo() == Repo
    end
  end
end
