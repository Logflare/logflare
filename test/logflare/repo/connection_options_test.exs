defmodule Logflare.Repo.ConnectionOptionsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.Cluster.PostgresStrategy
  alias Logflare.ContextCache.CacheBuster
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

  test "primary URL rebinds host-derived SNI when it replaces the configured hostname" do
    ssl = [
      verify: :verify_peer,
      cacertfile: "/custom/ca.pem",
      server_name_indication: :disable
    ]

    domain =
      ConnectionOptions.prepare(
        [
          url: "postgres://logflare@database.example.com/logflare?ssl=true",
          hostname: "127.0.0.1",
          ssl: ssl
        ],
        :primary
      )

    refute Keyword.has_key?(domain[:ssl], :server_name_indication)

    ip =
      ConnectionOptions.prepare(
        [url: "postgres://logflare@127.0.0.1/logflare?ssl=true"],
        :primary
      )

    assert ip[:ssl][:cacerts] == :public_key.cacerts_get()
    assert ip[:ssl][:server_name_indication] == :disable
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

  test "primary and replica URLs reject nested URL query parameters" do
    nested_url =
      "postgres://nested_user:nested_secret@nested.example/nested_database?auth=aws_iam"

    url =
      "postgres://outer_user:outer_secret@outer.example/outer_database?url=#{URI.encode_www_form(nested_url)}"

    error =
      assert_raise ArgumentError, ~r/`url` query parameter is not supported/, fn ->
        ConnectionOptions.prepare([url: url], :primary)
      end

    refute Exception.message(error) =~ "outer_secret"
    refute Exception.message(error) =~ "nested_secret"

    assert {:error, "`url` query parameter is not supported"} = Replicas.parse(url)

    replica_error =
      assert_raise ArgumentError, ~r/`url` query parameter is not supported/, fn ->
        Replicas.parse!(url)
      end

    refute Exception.message(replica_error) =~ "outer_secret"
    refute Exception.message(replica_error) =~ "nested_secret"
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

    query_password_error =
      assert_raise Ecto.InvalidURLError, fn ->
        ConnectionOptions.prepare(
          [
            url:
              "postgres://url_user@database.example.com/logflare?password=query_secret&pool_size=invalid"
          ],
          :primary
        )
      end

    refute Exception.message(query_password_error) =~ "query_secret"
    refute query_password_error.url =~ "query_secret"
    assert query_password_error.url =~ "password=REDACTED"

    replica_query_error =
      assert_raise ArgumentError, fn ->
        Replicas.parse!(
          "postgres://url_user@database.example.com/logflare?password=query_secret&pool_size=invalid"
        )
      end

    refute Exception.message(replica_query_error) =~ "query_secret"
    assert Exception.message(replica_query_error) =~ "password=REDACTED"

    query_auth_error =
      assert_raise ArgumentError, fn ->
        ConnectionOptions.prepare(
          [
            url:
              "postgres://url_user@database.example.com/logflare?password=query_secret&auth=aws_iam"
          ],
          :primary
        )
      end

    refute Exception.message(query_auth_error) =~ "query_secret"
    assert Exception.message(query_auth_error) =~ "password=REDACTED"
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

  describe "prepare_replication_epgsql/2" do
    setup do
      previous_path = Application.fetch_env(:logflare, :rds_ca_cert_path)
      previous_access_key_id = Application.fetch_env(:ex_aws, :access_key_id)
      previous_secret_access_key = Application.fetch_env(:ex_aws, :secret_access_key)
      previous_security_token = Application.fetch_env(:ex_aws, :security_token)
      previous_rds_config = Application.fetch_env(:ex_aws, :rds)
      ca_bundle_path = write_ca_bundle!()

      Application.put_env(:logflare, :rds_ca_cert_path, ca_bundle_path)
      Application.put_env(:ex_aws, :access_key_id, "AKIAIOSFODNN7EXAMPLE")
      Application.put_env(:ex_aws, :secret_access_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
      Application.delete_env(:ex_aws, :security_token)
      Application.delete_env(:ex_aws, :rds)

      on_exit(fn ->
        restore_application_env(:logflare, :rds_ca_cert_path, previous_path)
        restore_application_env(:ex_aws, :access_key_id, previous_access_key_id)
        restore_application_env(:ex_aws, :secret_access_key, previous_secret_access_key)
        restore_application_env(:ex_aws, :security_token, previous_security_token)
        restore_application_env(:ex_aws, :rds, previous_rds_config)
      end)

      password_primary = [
        hostname: "primary.example.com",
        port: 6432,
        username: "logflare",
        password: "primary_secret",
        database: "logflare",
        ssl: [
          verify: :verify_peer,
          cacertfile: "/certs/db-server-ca.pem",
          certfile: "/certs/db-client-cert.pem",
          keyfile: "/certs/db-client-key.pem"
        ]
      ]

      iam_primary = [
        hostname: "logflare-proxy.proxy-abc.eu-west-1.rds.amazonaws.com",
        username: "logflare",
        database: "logflare",
        logflare_auth: :aws_iam,
        logflare_aws_region: "eu-west-1"
      ]

      %{
        password_primary: password_primary,
        iam_primary: iam_primary,
        writer: "logflare.cluster-abc.eu-west-1.rds.amazonaws.com"
      }
    end

    test "uses the primary connection without replication URL options", context do
      assert ConnectionOptions.prepare_replication_epgsql(context.password_primary, nil) ==
               ConnectionOptions.prepare_epgsql(context.password_primary)
    end

    test "connects to the writer with AWS IAM authentication from the URL", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://iam_user@#{context.writer}:5433/logflare?auth=aws_iam&aws_region=us-east-1&ssl=true"
        )

      epgsql =
        ConnectionOptions.prepare_replication_epgsql(context.password_primary, url_options)

      assert epgsql.host == String.to_charlist(context.writer)
      assert epgsql.port == 5433
      assert epgsql.username == "iam_user"
      assert epgsql.database == "logflare"
      assert epgsql.ssl == :required
      assert epgsql.ssl_opts[:verify] == :verify_peer
      assert epgsql.ssl_opts[:server_name_indication] == String.to_charlist(context.writer)
      assert epgsql.password.() =~ "%2Fus-east-1%2Frds-db%2F"
    end

    test "a URL with auth=aws_iam and no region uses the primary region", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://logflare@#{context.writer}/logflare?auth=aws_iam"
        )

      epgsql = ConnectionOptions.prepare_replication_epgsql(context.iam_primary, url_options)

      assert epgsql.host == String.to_charlist(context.writer)
      assert epgsql.password.() =~ "%2Feu-west-1%2Frds-db%2F"
    end

    test "a URL region replaces the inherited primary region", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://logflare@#{context.writer}/logflare?aws_region=us-east-1"
        )

      epgsql = ConnectionOptions.prepare_replication_epgsql(context.iam_primary, url_options)

      assert epgsql.password.() =~ "%2Fus-east-1%2Frds-db%2F"
    end

    test "does not send primary client certificates when the URL sets authentication",
         context do
      for url <- [
            "postgres://iam_user@#{context.writer}/logflare?auth=aws_iam&aws_region=eu-west-1",
            "postgres://logflare:secret@#{context.writer}/logflare?ssl=true"
          ] do
        url_options = ConnectionOptions.parse_replication_url!(url)

        epgsql =
          ConnectionOptions.prepare_replication_epgsql(context.password_primary, url_options)

        for key <- [:certfile, :keyfile, :cacertfile] do
          refute Keyword.has_key?(epgsql.ssl_opts, key)
        end
      end
    end

    test "a URL with a password and ssl=true trusts the RDS CA bundle", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://replicator:replication_secret@#{context.writer}/logflare?ssl=true"
        )

      epgsql =
        ConnectionOptions.prepare_replication_epgsql(context.password_primary, url_options)

      assert epgsql.username == "replicator"
      assert epgsql.password == "replication_secret"
      assert epgsql.ssl == :required
      assert epgsql.ssl_opts[:verify] == :verify_peer
      assert length(epgsql.ssl_opts[:cacerts]) == length(:public_key.cacerts_get()) + 1
    end

    test "ssl=true trusts the RDS CA bundle for IP literal hosts", context do
      for {primary, url} <- [
            {context.password_primary,
             "postgres://replicator:replication_secret@10.0.0.5/logflare?ssl=true"},
            {context.password_primary,
             "postgres://replicator:replication_secret@[2001:db8::5]/logflare?ssl=true"},
            {Keyword.put(context.password_primary, :ssl, true), "10.0.0.5"}
          ] do
        url_options = ConnectionOptions.parse_replication_url!(url)
        epgsql = ConnectionOptions.prepare_replication_epgsql(primary, url_options)

        assert epgsql.ssl == :required
        assert epgsql.ssl_opts[:verify] == :verify_peer
        assert epgsql.ssl_opts[:server_name_indication] == :disable
        assert length(epgsql.ssl_opts[:cacerts]) == length(:public_key.cacerts_get()) + 1
      end
    end

    test "a URL password without ssl connects without TLS", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://replicator:replication_secret@#{context.writer}/logflare"
        )

      epgsql =
        ConnectionOptions.prepare_replication_epgsql(context.password_primary, url_options)

      assert epgsql.password == "replication_secret"
      refute Map.has_key?(epgsql, :ssl)
    end

    test "a URL password replaces primary AWS IAM authentication", context do
      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://replicator:replication_secret@#{context.writer}/logflare"
        )

      epgsql = ConnectionOptions.prepare_replication_epgsql(context.iam_primary, url_options)

      assert epgsql.password == "replication_secret"
    end

    test "a bare host inherits primary authentication and TLS", context do
      url_options = ConnectionOptions.parse_replication_url!(context.writer)

      epgsql =
        ConnectionOptions.prepare_replication_epgsql(context.password_primary, url_options)

      assert epgsql.host == String.to_charlist(context.writer)
      refute Map.has_key?(epgsql, :port)
      assert epgsql.username == "logflare"
      assert epgsql.password == "primary_secret"
      assert epgsql.database == "logflare"
      assert epgsql.ssl == :required
      assert epgsql.ssl_opts[:certfile] == "/certs/db-client-cert.pem"
    end

    test "rejects an RDS Proxy host" do
      error =
        assert_raise ArgumentError, fn ->
          ConnectionOptions.parse_replication_url!(
            "postgres://logflare@logflare-proxy.proxy-abc.eu-west-1.rds.amazonaws.com/logflare?auth=aws_iam&aws_region=eu-west-1"
          )
        end

      assert Exception.message(error) =~ "RDS Proxy does not support replication connections"
    end

    test "names the variable and redacts credentials in invalid URL errors", context do
      error =
        assert_raise ArgumentError, fn ->
          ConnectionOptions.parse_replication_url!(
            "postgres://logflare:hunter2@#{context.writer}/logflare?auth=kerberos"
          )
        end

      refute Exception.message(error) =~ "hunter2"
      assert Exception.message(error) =~ "invalid LOGFLARE_CACHE_BUSTER_DB_URL"
    end

    test "the cache buster connects with the configured replication URL", context do
      previous_cache_buster = Application.fetch_env(:logflare, CacheBuster)
      previous_enable_cainophile = Application.fetch_env(:logflare, :enable_cainophile)

      url_options =
        ConnectionOptions.parse_replication_url!(
          "postgres://replicator:replication_secret@#{context.writer}/logflare"
        )

      Application.put_env(
        :logflare,
        CacheBuster,
        Keyword.put(Application.get_env(:logflare, CacheBuster, []), :db_url_options, url_options)
      )

      Application.put_env(:logflare, :enable_cainophile, true)

      on_exit(fn ->
        restore_application_env(:logflare, CacheBuster, previous_cache_buster)
        restore_application_env(:logflare, :enable_cainophile, previous_enable_cainophile)
      end)

      epgsql = cainophile_epgsql_options()

      assert epgsql.host == String.to_charlist(context.writer)
      assert epgsql.username == "replicator"
      assert epgsql.password == "replication_secret"
    end
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

  defp write_ca_bundle! do
    {:cert, der, _} = hd(:public_key.cacerts_get())

    path =
      Path.join(System.tmp_dir!(), "logflare-rds-ca-#{System.unique_integer([:positive])}.pem")

    File.write!(path, :public_key.pem_encode([{:Certificate, der, :not_encrypted}]))
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp restore_application_env(app, key, {:ok, value}), do: Application.put_env(app, key, value)
  defp restore_application_env(app, key, :error), do: Application.delete_env(app, key)
end
