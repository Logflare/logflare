defmodule Logflare.DbOutageToxiproxyTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Factory
  alias Logflare.Repo
  alias Logflare.TestUtils.Toxiproxy
  alias Logflare.User
  alias Logflare.Users

  @moduletag :toxiproxy
  @moduletag timeout: 90_000

  setup do
    user = Sandbox.unboxed_run(Repo, fn -> Factory.insert(:user) end)

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn -> Repo.delete!(user) end)
    end)

    config = Repo.config()
    upstream = "#{Keyword.fetch!(config, :hostname)}:#{Keyword.get(config, :port, 5432)}"
    proxy = Toxiproxy.create!(upstream)
    peer = start_peer(proxy, config)

    assert %User{id: id} = call(peer, Users.Cache, :get, [user.id])
    assert id == user.id

    processes =
      for name <- [Logflare.Supervisor, Repo, Users.Cache], into: %{} do
        pid = call(peer, Process, :whereis, [name])
        assert is_pid(pid)
        {name, pid}
      end

    {:ok, peer: peer, proxy: proxy, user: user, processes: processes}
  end

  test "cached reads survive resets and an uncached read recovers without caching the failure", %{
    peer: peer,
    proxy: proxy,
    user: %{id: id},
    processes: processes
  } do
    Toxiproxy.add_toxic!(proxy, %{
      name: "disconnect",
      type: "reset_peer",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{timeout: 0}
    })

    assert %User{id: ^id} = call(peer, Users.Cache, :get, [id])
    assert {:ok, true} = call(peer, Cachex, :del, [Users.Cache, {:get, [id]}])
    assert {:error, :database_unavailable} = call(peer, Users.Cache, :get, [id])
    assert {:ok, nil} = call(peer, Cachex, :get, [Users.Cache, {:get, [id]}])
    assert_processes_unchanged(peer, processes)

    Toxiproxy.remove_toxic!(proxy, "disconnect")

    assert_user_recovers(peer, id, System.monotonic_time(:millisecond) + 20_000)

    assert {:ok, {:cached, %User{id: ^id}}} =
             call(peer, Cachex, :get, [Users.Cache, {:get, [id]}])

    assert_processes_unchanged(peer, processes)
  end

  test "a stalled established connection can fail and recover without restarting the application",
       %{
         peer: peer,
         proxy: proxy,
         user: %{id: id},
         processes: processes
       } do
    assert {:ok, %{rows: [[1]]}} =
             call(peer, Repo, :query, ["SELECT 1", [], [timeout: 1_000, queue: false]])

    Toxiproxy.add_toxic!(proxy, %{
      name: "stall",
      type: "timeout",
      stream: "downstream",
      toxicity: 1.0,
      attributes: %{timeout: 0}
    })

    task =
      Task.async(fn ->
        call(peer, Repo, :query, ["SELECT 1", [], [timeout: 500, queue: false]])
      end)

    assert Task.yield(task, 100) == nil
    assert %User{id: ^id} = call(peer, Users.Cache, :get, [id])
    result = Task.yield(task, 600)
    assert_processes_unchanged(peer, processes)

    Toxiproxy.remove_toxic!(proxy, "stall")
    Toxiproxy.enable!(proxy, false)
    Toxiproxy.enable!(proxy, true)

    assert {:ok, {:error, %DBConnection.ConnectionError{}}} = result || Task.yield(task, 10_000)
    assert {:ok, true} = call(peer, Cachex, :del, [Users.Cache, {:get, [id]}])
    assert_user_recovers(peer, id, System.monotonic_time(:millisecond) + 20_000)
    assert_processes_unchanged(peer, processes)
  end

  @spec start_peer(map(), keyword()) :: pid()
  defp start_peer(proxy, config) do
    {:ok, peer, _node} =
      :peer.start(%{
        connection: :standard_io,
        args: [~c"+S", ~c"2:2"],
        wait_boot: 60_000
      })

    on_exit(fn ->
      if Process.alive?(peer), do: :peer.stop(peer)
    end)

    :ok = :peer.call(peer, :code, :add_paths, [:code.get_path()])
    {:ok, _apps} = :peer.call(peer, :application, :ensure_all_started, [:elixir])

    environment =
      for {app, _description, _version} <- Application.loaded_applications() do
        {app, Application.get_all_env(app)}
      end

    :ok = call(peer, Application, :put_all_env, [environment, [persistent: true]])

    repo_config =
      config
      |> Keyword.drop([:url, :socket, :socket_dir, :name])
      |> Keyword.merge(
        hostname: proxy.host,
        port: proxy.port,
        pool: DBConnection.ConnectionPool,
        pool_size: 4,
        queue_target: 2_000,
        queue_interval: 1_000,
        timeout: 30_000
      )

    for {key, value} <- [
          {Repo, repo_config},
          {:env, :test},
          {:read_replicas, []},
          {:enable_cainophile, false},
          {:single_tenant, false},
          {:supabase_mode, false},
          {LogflareWeb.Endpoint,
           Application.fetch_env!(:logflare, LogflareWeb.Endpoint) |> Keyword.put(:server, false)}
        ] do
      :ok = call(peer, Application, :put_env, [:logflare, key, value, [persistent: true]])
    end

    :ok =
      call(peer, Application, :put_env, [
        :logger,
        :default_handler,
        [filters: [], level: :error],
        [persistent: true]
      ])

    {:ok, _apps} = :peer.call(peer, Application, :ensure_all_started, [:logflare], 60_000)
    peer
  end

  @spec call(pid(), module(), atom(), list()) :: term()
  defp call(peer, module, function, args) do
    :peer.call(peer, module, function, args, 40_000)
  end

  @spec assert_processes_unchanged(pid(), %{module() => pid()}) :: :ok
  defp assert_processes_unchanged(peer, processes) do
    for {name, pid} <- processes do
      assert call(peer, Process, :whereis, [name]) == pid
    end

    :ok
  end

  @spec assert_user_recovers(pid(), integer(), integer()) :: :ok
  defp assert_user_recovers(peer, id, deadline) do
    case call(peer, Users.Cache, :get, [id]) do
      %User{id: ^id} ->
        :ok

      {:error, :database_unavailable} ->
        assert System.monotonic_time(:millisecond) < deadline,
               "user #{id} did not become readable after restoring the proxy"

        Process.sleep(100)
        assert_user_recovers(peer, id, deadline)
    end
  end
end
