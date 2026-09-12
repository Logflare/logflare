defmodule Logflare.Repo.ConnectionOptionsTest do
  use ExUnit.Case, async: false

  alias Logflare.Cluster.PostgresStrategy
  alias Logflare.ContextCache.Supervisor, as: ContextCacheSupervisor
  alias Logflare.GenSingleton
  alias Logflare.Repo
  alias Logflare.Repo.ConnectionOptions

  test "password clients preserve connection options without inheriting Ecto pools" do
    ssl = [
      verify: :verify_peer,
      cacertfile: "/custom/ca.pem",
      server_name_indication: :disable
    ]

    config = [
      hostname: "database.example.com",
      port: 5433,
      username: "logflare",
      password: "secret",
      database: "logflare",
      socket_options: [keepalive: true],
      ssl: ssl,
      name: Repo,
      pool: Ecto.Adapters.SQL.Sandbox,
      pool_count: 2,
      pool_size: 27,
      queue_interval: 1_234,
      queue_target: 5_678,
      start_apps_before_migration: [:ssl]
    ]

    postgrex = ConnectionOptions.prepare_postgrex(config)

    for key <- [:hostname, :port, :username, :password, :database, :socket_options, :ssl] do
      assert postgrex[key] == config[key]
    end

    for key <- [
          :name,
          :pool,
          :pool_count,
          :pool_size,
          :queue_interval,
          :queue_target,
          :start_apps_before_migration
        ] do
      refute Keyword.has_key?(postgrex, key)
    end

    epgsql = ConnectionOptions.prepare_epgsql(config)

    assert epgsql.host == ~c"database.example.com"
    assert epgsql.port == 5433
    assert epgsql.username == "logflare"
    assert epgsql.password == "secret"
    assert epgsql.database == "logflare"
    assert epgsql.tcp_opts == [keepalive: true]
    assert epgsql.ssl == :required
    assert epgsql.ssl_opts[:verify] == :verify_peer
    assert epgsql.ssl_opts[:cacertfile] == "/custom/ca.pem"
    assert epgsql.ssl_opts[:server_name_indication] == :disable
  end

  test "direct database clients use Ecto-resolved URL configuration" do
    previous_repo_config = Application.fetch_env(:logflare, Repo)
    previous_enable_cainophile = Application.fetch_env(:logflare, :enable_cainophile)

    Application.put_env(
      :logflare,
      Repo,
      url: "postgres://postgres:postgres@localhost:5432/logflare_test",
      pool: Ecto.Adapters.SQL.Sandbox,
      pool_size: 27,
      queue_interval: 1_234,
      queue_target: 5_678
    )

    Application.put_env(:logflare, :enable_cainophile, true)

    on_exit(fn ->
      restore_application_env(:logflare, Repo, previous_repo_config)
      restore_application_env(:logflare, :enable_cainophile, previous_enable_cainophile)
    end)

    postgrex = PostgresStrategy.get_db_options()

    assert postgrex[:hostname] == "localhost"
    assert postgrex[:port] == 5432
    assert postgrex[:username] == "postgres"
    assert postgrex[:password] == "postgres"
    assert postgrex[:database] == "logflare_test"
    refute Keyword.has_key?(postgrex, :url)

    for key <- [:pool, :pool_size, :queue_interval, :queue_target] do
      refute Keyword.has_key?(postgrex, key)
    end

    postgrex_conn = start_supervised!({Postgrex, postgrex})
    assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(postgrex_conn, "SELECT 1", [])

    epgsql = cainophile_epgsql_options()
    assert epgsql.host == ~c"localhost"
    assert epgsql.port == 5432
    assert epgsql.username == "postgres"
    assert epgsql.password == "postgres"
    assert epgsql.database == "logflare_test"

    assert {:ok, epgsql_conn} = :epgsql.connect(epgsql)
    on_exit(fn -> if Process.alive?(epgsql_conn), do: :epgsql.close(epgsql_conn) end)

    assert {:ok, _columns, [{"1"}]} = :epgsql.squery(epgsql_conn, "SELECT 1")
  end

  defp cainophile_epgsql_options do
    assert {:ok, {_flags, children}} = ContextCacheSupervisor.init([])

    singleton =
      Enum.find(children, fn child ->
        match?(%{start: {GenSingleton, :start_link, _args}}, child)
      end)

    assert %{start: {GenSingleton, :start_link, [[child_spec: cainophile_spec]]}} = singleton

    assert {Cainophile.Adapters.Postgres, :start_link, [cainophile_options]} =
             cainophile_spec.start

    Keyword.fetch!(cainophile_options, :epgsql)
  end

  defp restore_application_env(app, key, {:ok, value}), do: Application.put_env(app, key, value)
  defp restore_application_env(app, key, :error), do: Application.delete_env(app, key)
end
