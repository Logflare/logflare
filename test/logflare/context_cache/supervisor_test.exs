defmodule Logflare.ContextCache.SupervisorTest do
  use ExUnit.Case, async: false

  alias Logflare.ContextCache.CacheBuster
  alias Logflare.ContextCache.Supervisor
  alias Logflare.Repo
  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas

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

  describe "wal_connection_config/0" do
    test "follows the primary by default" do
      put_env(:read_replicas, [Replicas.parse!("replica.example.com")])
      prefer_read_replica(false)

      assert Supervisor.wal_connection_config() == Repo.config()
    end

    test "follows the primary when preferring replicas but none are configured" do
      put_env(:read_replicas, [])
      prefer_read_replica(true)

      assert Supervisor.wal_connection_config() == Repo.config()
    end

    test "follows the first read replica when preferred" do
      put_env(:read_replicas, [
        Replicas.parse!("postgres://wal:secret@replica1.example.com:5433/replica_db"),
        Replicas.parse!("replica2.example.com")
      ])

      prefer_read_replica(true)

      config = Supervisor.wal_connection_config()

      assert config[:hostname] == "replica1.example.com"
      assert config[:port] == 5433
      assert config[:database] == "replica_db"
      assert config[:username] == "wal"

      assert %{host: ~c"replica1.example.com", port: 5433, database: "replica_db"} =
               ConnectionOptions.prepare_epgsql(config)
    end

    test "inherits omitted replica settings from the primary" do
      put_env(:read_replicas, [Replicas.parse!("replica.example.com")])
      prefer_read_replica(true)

      config = Supervisor.wal_connection_config()

      assert config[:hostname] == "replica.example.com"
      assert config[:database] == Repo.config()[:database]
      assert config[:username] == Repo.config()[:username]
    end
  end
end
