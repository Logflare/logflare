defmodule Logflare.Repo.ConnectionOptionsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cluster.PostgresStrategy
  alias Logflare.ContextCache.Supervisor, as: ContextCacheSupervisor
  alias Logflare.GenSingleton
  alias Logflare.Repo
  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas

  test "primary URL values override individual connection options" do
    prepared =
      ConnectionOptions.prepare(
        [
          url:
            "postgres://url_user:url%20secret@[::1]:5433/url_database?auth=password&pool_size=7&ssl=false",
          hostname: "configured.example.com",
          port: 5432,
          username: "configured_user",
          password: "configured_secret",
          database: "configured_database",
          pool_size: 3,
          socket_options: [:inet],
          logflare_auth: :aws_iam,
          logflare_aws_region: "us-east-1"
        ],
        :primary
      )

    assert prepared[:hostname] == "::1"
    assert prepared[:port] == 5433
    assert prepared[:username] == "url_user"
    assert prepared[:password] == "url secret"
    assert prepared[:database] == "url_database"
    assert prepared[:pool_size] == 7
    assert prepared[:socket_options] == [:inet6]
    assert prepared[:ssl] == false
    refute Keyword.has_key?(prepared, :url)
    refute Keyword.has_key?(prepared, :auth)
    refute Keyword.has_key?(prepared, :aws_region)
    refute Keyword.has_key?(prepared, :logflare_auth)
    refute Keyword.has_key?(prepared, :logflare_aws_region)
    refute Keyword.has_key?(prepared, :configure)
  end

  test "primary URL preserves configured SSL options when ssl=true" do
    ssl = [verify: :verify_peer, cacertfile: "/custom/ca.pem"]

    log =
      capture_log(fn ->
        prepared =
          ConnectionOptions.prepare(
            [
              url: "postgres://logflare@database.example.com/logflare?ssl=true",
              ssl: ssl
            ],
            :primary
          )

        assert prepared[:ssl] == ssl
      end)

    assert log =~ "ignoring `ssl=true` parameter in URL"
  end

  test "primary URL password authentication can reuse the configured password" do
    prepared =
      ConnectionOptions.prepare(
        [
          url: "postgres://url_user@database.example.com/url_database?auth=password",
          password: "configured_secret",
          logflare_auth: :aws_iam,
          logflare_aws_region: "us-east-1"
        ],
        :primary
      )

    assert prepared[:username] == "url_user"
    assert prepared[:password] == "configured_secret"
    refute Keyword.has_key?(prepared, :configure)
  end

  test "primary URL errors redact credentials" do
    invalid_url = "postgres://url_user:supersecret@database.example.com"

    invalid_url_error =
      assert_raise Ecto.InvalidURLError, fn ->
        ConnectionOptions.prepare([url: invalid_url], :primary)
      end

    refute Exception.message(invalid_url_error) =~ "supersecret"
    refute invalid_url_error.url =~ "supersecret"

    auth_error =
      assert_raise ArgumentError, fn ->
        ConnectionOptions.prepare(
          [
            url: "postgres://url_user:supersecret@database.example.com/logflare?auth=aws_iam"
          ],
          :primary
        )
      end

    assert Exception.message(auth_error) =~ "auth=aws_iam cannot be combined with a password"
    refute Exception.message(auth_error) =~ "supersecret"
  end

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

  test "replicas inherit primary URL options without inheriting its host" do
    previous_repo_config = Application.fetch_env(:logflare, Repo)

    Application.put_env(
      :logflare,
      Repo,
      url: "postgres://postgres:postgres@localhost:5432/logflare_test?auth=password",
      pool: DBConnection.ConnectionPool,
      pool_size: 1
    )

    on_exit(fn -> restore_application_env(:logflare, Repo, previous_repo_config) end)

    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    entry = Replicas.parse!("127.0.0.1")
    start_supervised!({Replicas, entries: [entry]})

    assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
    assert opts[:hostname] == "127.0.0.1"
    assert opts[:username] == "postgres"
    assert opts[:password] == "postgres"
    assert opts[:database] == "logflare_test"
    assert opts[:socket_options] == [:inet]
    refute Keyword.has_key?(opts, :url)
    refute Keyword.has_key?(opts, :auth)
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
