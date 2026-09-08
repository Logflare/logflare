defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Logflare.Repo
  alias Logflare.Repo.AwsIam
  alias Logflare.Repo.ConnectionOptions
  alias Logflare.Repo.Replicas

  defp start_read_replicas(raw_entries, entry_overrides \\ []) do
    primary_hostname = Keyword.fetch!(Repo.config(), :hostname)

    entries =
      Enum.map(raw_entries, fn entry ->
        {key, config} = Replicas.parse!(entry)
        {key, Keyword.merge(config, entry_overrides)}
      end)

    for {_key, config} <- entries do
      refute config[:hostname] == primary_hostname,
             "replica hostname #{config[:hostname]} should be different from primary hostname"
    end

    # apply_with_replica/3 reads the configured entries from the application environment.
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)

    # Observe the effective Ecto configuration for each replica pool.
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    start_result = start_supervised!({Replicas, entries: entries})

    for {_key, config} <- entries do
      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}

      for {k, v} <- config,
          k not in [:ssl, :logflare_auth, :logflare_aws_region] do
        assert Keyword.fetch!(opts, k) == v
      end

      refute Keyword.has_key?(opts, :logflare_connection_role)
      refute Keyword.has_key?(opts, :logflare_auth)
      refute Keyword.has_key?(opts, :logflare_aws_region)

      assert {Replicas, :after_connect, [_primary_after_connect]} =
               Keyword.fetch!(opts, :after_connect)
    end

    start_result
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

      assert Repo.get_dynamic_repo() == Repo
    end

    test "makes replica pool connections read-only" do
      start_read_replicas(["127.0.0.1"],
        pool: DBConnection.ConnectionPool,
        pool_size: 1
      )

      assert %Postgrex.Result{rows: [["on"]]} =
               Repo.apply_with_replica(
                 Repo,
                 :query!,
                 ["SHOW default_transaction_read_only", []]
               )

      assert_raise Postgrex.Error, ~r/read-only transaction/, fn ->
        Repo.apply_with_replica(
          Repo,
          :query!,
          ["CREATE TEMP TABLE read_only_replica_probe (id integer)", []]
        )
      end
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

  describe "Replicas.after_connect/2" do
    setup do
      opts = Keyword.take(Repo.config(), [:hostname, :port, :username, :password, :database])

      %{conn: start_supervised!({Postgrex, opts})}
    end

    test "opens the session read-only", %{conn: conn} do
      Replicas.after_connect(conn, _no_primary_hook = nil)

      assert %Postgrex.Result{rows: [["on"]]} =
               Postgrex.query!(conn, "SHOW default_transaction_read_only", [])
    end

    test "rejects writes on the session", %{conn: conn} do
      Replicas.after_connect(conn, _no_primary_hook = nil)

      assert_raise Postgrex.Error, ~r/read-only transaction/, fn ->
        Postgrex.query!(conn, "CREATE TEMP TABLE read_only_probe (id integer)", [])
      end
    end

    test "still runs the primary's after_connect, given as an MFA", %{conn: conn} do
      Replicas.after_connect(conn, {Postgrex, :query!, ["SET application_name = 'mfa'", []]})

      assert %Postgrex.Result{rows: [["mfa"]]} =
               Postgrex.query!(conn, "SHOW application_name", [])

      assert %Postgrex.Result{rows: [["on"]]} =
               Postgrex.query!(conn, "SHOW default_transaction_read_only", [])
    end

    test "still runs the primary's after_connect, given as a function", %{conn: conn} do
      hook = fn conn -> Postgrex.query!(conn, "SET application_name = 'fun'", []) end

      Replicas.after_connect(conn, hook)

      assert %Postgrex.Result{rows: [["fun"]]} =
               Postgrex.query!(conn, "SHOW application_name", [])

      assert %Postgrex.Result{rows: [["on"]]} =
               Postgrex.query!(conn, "SHOW default_transaction_read_only", [])
    end
  end

  describe "AWS IAM authentication" do
    setup do
      previous_access_key_id = Application.fetch_env(:ex_aws, :access_key_id)
      previous_secret_access_key = Application.fetch_env(:ex_aws, :secret_access_key)
      previous_path = Application.fetch_env(:logflare, :rds_ca_cert_path)
      {path, certificate} = write_ca_bundle!()

      Application.put_env(:ex_aws, :access_key_id, "AKIAIOSFODNN7EXAMPLE")
      Application.put_env(:ex_aws, :secret_access_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
      Application.put_env(:logflare, :rds_ca_cert_path, path)

      on_exit(fn ->
        restore_application_env(:ex_aws, :access_key_id, previous_access_key_id)
        restore_application_env(:ex_aws, :secret_access_key, previous_secret_access_key)
        restore_application_env(:logflare, :rds_ca_cert_path, previous_path)
      end)

      %{certificate: certificate, host: "database.example.com", region: "eu-west-1"}
    end

    test "replica URIs normalize Logflare-specific IAM options", %{host: host, region: region} do
      assert {:ok, {_key, config}} =
               Replicas.parse(
                 "postgres://logflare@#{host}:5432/logflare?auth=aws_iam&aws_region=#{region}&ssl=true"
               )

      assert config[:logflare_auth] == :aws_iam
      assert config[:logflare_aws_region] == region
      assert config[:username] == "logflare"
      refute Keyword.has_key?(config, :auth)
      refute Keyword.has_key?(config, :aws_region)
      refute Keyword.has_key?(config, :password)
    end

    test "a replica password explicitly overrides inherited IAM", %{host: host} do
      assert {:ok, {_key, config}} =
               Replicas.parse("postgres://logflare:secret@#{host}/logflare")

      assert config[:logflare_auth] == :password
      assert config[:password] == "secret"
    end

    test "replica URIs reject invalid authentication options", %{host: host} do
      assert {:error, reason} =
               Replicas.parse("postgres://logflare@#{host}/logflare?auth=kerberos")

      assert reason =~ "unsupported auth=kerberos"
      assert reason =~ ~s(expected "aws_iam")

      assert {:error, reason} =
               Replicas.parse(
                 "postgres://logflare:secret@#{host}/logflare?auth=aws_iam&aws_region=eu-west-1"
               )

      assert reason =~ "cannot be combined with a password"

      assert {:error, "aws_region cannot be empty"} =
               Replicas.parse("postgres://logflare@#{host}/logflare?auth=aws_iam&aws_region=")
    end

    test "primary and replica connections use the same IAM configuration path", context do
      %{certificate: certificate, host: host, region: region} = context
      after_connect = {Postgrex, :query!, ["SET application_name = 'configured'", []]}

      base_config = [
        hostname: host,
        username: "logflare",
        after_connect: after_connect,
        logflare_auth: :aws_iam,
        logflare_aws_region: region
      ]

      for role <- [:primary, :replica] do
        config = Keyword.put(base_config, :logflare_connection_role, role)
        assert {:ok, prepared} = Repo.init(:supervisor, config)

        assert {AwsIam, :configure, [^region, nil]} = prepared[:configure]
        assert certificate in prepared[:ssl][:cacerts]
        refute Keyword.has_key?(prepared[:ssl], :server_name_indication)
        refute Keyword.has_key?(prepared, :logflare_connection_role)
        refute Keyword.has_key?(prepared, :logflare_auth)
        refute Keyword.has_key?(prepared, :logflare_aws_region)

        case role do
          :primary ->
            assert prepared[:after_connect] == after_connect

          :replica ->
            assert {Replicas, :after_connect, [^after_connect]} = prepared[:after_connect]
        end
      end
    end

    test "a replica pool installs IAM and read-only callbacks", %{host: host, region: region} do
      entries = [
        Replicas.parse!(
          "postgres://logflare@#{host}:5432/logflare?auth=aws_iam&aws_region=#{region}"
        )
      ]

      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      assert {AwsIam, :configure, [^region, _previous_configure]} = opts[:configure]
      assert {Replicas, :after_connect, [_primary_after_connect]} = opts[:after_connect]
      refute Keyword.has_key?(opts, :logflare_auth)
      refute Keyword.has_key?(opts, :logflare_aws_region)
    end

    test "password authentication leaves inherited callbacks unchanged" do
      configure = {__MODULE__, :configure_username, ["configured"]}

      prepared =
        ConnectionOptions.prepare(
          [
            configure: configure,
            logflare_auth: :password,
            logflare_aws_region: "ignored"
          ],
          :primary
        )

      assert prepared[:configure] == configure
      refute Keyword.has_key?(prepared, :logflare_auth)
      refute Keyword.has_key?(prepared, :logflare_aws_region)
    end

    test "IAM authentication rejects missing settings and insecure TLS", %{
      host: host,
      region: region
    } do
      base = [hostname: host, username: "logflare", logflare_auth: :aws_iam]

      assert_raise ArgumentError, ~r/requires an AWS region/, fn ->
        ConnectionOptions.prepare(base, :primary)
      end

      for ssl <- [false, "invalid", [verify: :verify_none]] do
        assert_raise ArgumentError, ~r/AWS IAM authentication requires/, fn ->
          base
          |> Keyword.put(:logflare_aws_region, region)
          |> Keyword.put(:ssl, ssl)
          |> ConnectionOptions.prepare(:primary)
        end
      end

      assert_raise ArgumentError, ~r/requires a DNS hostname/, fn ->
        base
        |> Keyword.put(:hostname, "127.0.0.1")
        |> Keyword.put(:logflare_aws_region, region)
        |> ConnectionOptions.prepare(:primary)
      end
    end

    test "IAM authentication preserves verified custom trust and resets inherited SNI", %{
      host: host,
      region: region
    } do
      ssl = [
        verify: :verify_peer,
        cacertfile: "/custom/ca.pem",
        server_name_indication: :disable
      ]

      prepared =
        ConnectionOptions.prepare(
          [
            hostname: host,
            username: "logflare",
            ssl: ssl,
            logflare_auth: :aws_iam,
            logflare_aws_region: region
          ],
          :primary
        )

      assert prepared[:ssl][:verify] == :verify_peer
      assert prepared[:ssl][:cacertfile] == "/custom/ca.pem"
      refute Keyword.has_key?(prepared[:ssl], :cacerts)
      refute Keyword.has_key?(prepared[:ssl], :server_name_indication)
    end

    test "an unavailable RDS bundle warns and falls back to system roots", %{
      host: host,
      region: region
    } do
      Application.put_env(:logflare, :rds_ca_cert_path, "/missing/rds-ca.pem")

      log =
        capture_log(fn ->
          prepared =
            ConnectionOptions.prepare(
              [
                hostname: host,
                username: "logflare",
                logflare_auth: :aws_iam,
                logflare_aws_region: region
              ],
              :primary
            )

          assert prepared[:ssl][:cacerts] == :public_key.cacerts_get()
        end)

      assert log =~ "AWS RDS CA bundle"
      assert log =~ "using system CA certificates"
    end

    test "auth_token/4 signs the configured region for a DNS endpoint", %{
      host: host,
      region: region
    } do
      token = AwsIam.auth_token(host, 5432, "logflare", region)

      assert String.starts_with?(token, "#{host}:5432/?")
      assert token =~ "Action=connect"
      assert token =~ "DBUser=logflare"
      assert token =~ "X-Amz-Signature="
      assert token =~ "X-Amz-Expires=900"
      assert token =~ "#{region}%2Frds-db"
    end

    test "auth_token/4 signs temporary environment credentials and normalizes hostnames", %{
      host: host,
      region: region
    } do
      previous_security_token = Application.fetch_env(:ex_aws, :security_token)
      previous_env = take_aws_credential_env()

      Application.delete_env(:ex_aws, :access_key_id)
      Application.delete_env(:ex_aws, :secret_access_key)
      Application.delete_env(:ex_aws, :security_token)
      System.put_env("AWS_ACCESS_KEY_ID", "ASIATEMPORARY")
      System.put_env("AWS_SECRET_ACCESS_KEY", "temporary-secret")
      System.put_env("AWS_SESSION_TOKEN", "session-token")

      on_exit(fn ->
        restore_aws_credential_env(previous_env)
        restore_application_env(:ex_aws, :security_token, previous_security_token)
      end)

      token = AwsIam.auth_token(String.upcase(host), 5432, "logflare", region)

      assert String.starts_with?(token, "#{host}:5432/?")
      assert token =~ "X-Amz-Security-Token=session-token"
    end

    test "auth_token/4 preserves a session token resolved by the credential provider", %{
      host: host,
      region: region
    } do
      previous_env = take_aws_credential_env()
      previous_security_token = Application.fetch_env(:ex_aws, :security_token)
      System.delete_env("AWS_ACCESS_KEY_ID")
      System.delete_env("AWS_SECRET_ACCESS_KEY")
      System.put_env("AWS_SESSION_TOKEN", "unrelated-session-token")
      Application.put_env(:ex_aws, :security_token, "provider-session-token")

      on_exit(fn ->
        restore_aws_credential_env(previous_env)
        restore_application_env(:ex_aws, :security_token, previous_security_token)
      end)

      token = AwsIam.auth_token(host, 5432, "logflare", region)
      assert token =~ "X-Amz-Security-Token=provider-session-token"
      refute token =~ "unrelated-session-token"
    end

    test "configure/3 replaces the password after inherited callbacks run", %{
      host: host,
      region: region
    } do
      opts = [hostname: host, port: 5432, username: "original", password: "stale"]

      callbacks = [
        {fn opts -> Keyword.put(opts, :username, "from_fun") end, "from_fun"},
        {{__MODULE__, :configure_username, ["from_mfa"]}, "from_mfa"}
      ]

      for {callback, expected_username} <- callbacks do
        configured = AwsIam.configure(opts, region, callback)

        assert configured[:username] == expected_username
        refute configured[:password] == "stale"
        assert configured[:password] =~ "DBUser=#{expected_username}"
      end
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

    test "parse!/1 redacts credentials in auth validation errors" do
      error =
        assert_raise ArgumentError, fn ->
          Replicas.parse!("postgres://u:supersecret@host?auth=kerberos")
        end

      refute Exception.message(error) =~ "supersecret"
    end
  end

  def configure_username(opts, username), do: Keyword.put(opts, :username, username)

  defp write_ca_bundle! do
    certificate = hd(:public_key.cacerts_get())
    {:cert, der, _} = certificate

    path =
      Path.join(System.tmp_dir!(), "logflare-rds-ca-#{System.unique_integer([:positive])}.pem")

    File.write!(path, :public_key.pem_encode([{:Certificate, der, :not_encrypted}]))
    on_exit(fn -> File.rm(path) end)
    {path, certificate}
  end

  defp take_aws_credential_env do
    for key <- ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"],
        into: %{},
        do: {key, System.get_env(key)}
  end

  defp restore_aws_credential_env(env) do
    Enum.each(env, fn {key, value} -> restore_system_env(key, value) end)
  end

  defp restore_application_env(app, key, {:ok, value}), do: Application.put_env(app, key, value)
  defp restore_application_env(app, key, :error), do: Application.delete_env(app, key)

  defp restore_system_env(key, nil), do: System.delete_env(key)
  defp restore_system_env(key, value), do: System.put_env(key, value)
end
