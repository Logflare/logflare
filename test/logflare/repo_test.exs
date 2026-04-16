defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(urls) do
    urls = urls |> List.wrap() |> Enum.map(&to_string/1)

    initial = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, urls)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, initial) end)

    start_supervised!({Replicas, urls: urls})
  end

  defp repo_url(repo, overrides \\ []) do
    %{
      username: username,
      password: password,
      hostname: hostname,
      port: port,
      database: database
    } = Map.new(repo.config())

    uri =
      %URI{
        scheme: "ecto",
        userinfo: username <> ":" <> password,
        host: hostname,
        port: port,
        path: "/" <> database
      }

    struct!(uri, overrides)
  end

  describe "apply_with_read_replica/3" do
    test "uses default repo when replicas list is empty" do
      start_read_replicas(_no_replicas = [])

      assert Repo.get_dynamic_repo() == Repo
      assert Repo.apply_with_read_replica(Repo, :get_dynamic_repo, []) == Repo
    end

    test "uses replica repo during execution and reverts afterward" do
      start_read_replicas([
        repo_url(Repo, scheme: "replica", query: "pool_size=1"),
        repo_url(Repo, scheme: "postgres", query: "pool_size=1")
      ])

      replica = Repo.apply_with_read_replica(Repo, :get_dynamic_repo, [])
      assert is_pid(replica)

      # and then it's back to the default
      assert Repo.get_dynamic_repo() == Repo
    end

    test "reverts repo if function raises" do
      start_read_replicas(repo_url(Repo))

      assert_raise ArithmeticError, fn ->
        Repo.apply_with_read_replica(Kernel, :/, [1, 0])
      end

      # still back to the default
      assert Repo.get_dynamic_repo() == Repo
    end
  end

  describe "current_role/0" do
    test "returns primary for the default repo" do
      assert Repo.current_role() == "primary"
    end

    test "returns replica when assigned a PID" do
      start_read_replicas(repo_url(Repo))

      assert Repo.apply_with_read_replica(Repo, :current_role, []) == "replica"
    end

    test "returns unknown for unexpected states" do
      Repo.put_dynamic_repo(:unexpected)
      assert Logflare.Repo.current_role() == "unknown"
    end
  end
end
