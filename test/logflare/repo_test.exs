defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
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

      for {k, v} <- config, k not in [:ssl, :auth] do
        assert Keyword.fetch!(opts, k) == v
      end

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

  describe "replica SSL configuration" do
    test "a DNS replica removes primary-specific SNI" do
      put_repo_config(ssl: [server_name_indication: :disable])

      entries = [Replicas.parse!("postgres://replica.example.com/logflare?ssl=true")]
      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      refute Keyword.has_key?(Keyword.fetch!(opts, :ssl), :server_name_indication)
    end

    test "an IP replica disables SNI without forwarding the primary value" do
      put_repo_config(ssl: [server_name_indication: "primary"])

      entries = [Replicas.parse!("postgres://127.0.0.2/logflare?ssl=true")]
      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      assert Keyword.fetch!(Keyword.fetch!(opts, :ssl), :server_name_indication) == :disable
    end
  end

  describe "IAM authentication" do
    setup do
      # Signing requires credentials but does not contact AWS.
      previous_access_key_id = Application.fetch_env(:ex_aws, :access_key_id)
      previous_secret_access_key = Application.fetch_env(:ex_aws, :secret_access_key)
      Application.put_env(:ex_aws, :access_key_id, "AKIAIOSFODNN7EXAMPLE")
      Application.put_env(:ex_aws, :secret_access_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

      on_exit(fn ->
        restore_application_env(:ex_aws, :access_key_id, previous_access_key_id)
        restore_application_env(:ex_aws, :secret_access_key, previous_secret_access_key)
      end)

      %{host: "logflare-proxy.proxy-abc123.eu-west-1.rds.amazonaws.com"}
    end

    test "parse/1 accepts auth=iam and carries no password", %{host: host} do
      assert {:ok, {_key, config}} =
               Replicas.parse("postgres://logflare@#{host}:5432/logflare?auth=iam&ssl=true")

      assert config[:auth] == :iam
      assert config[:username] == "logflare"
      refute Keyword.has_key?(config, :password)
    end

    test "parse/1 rejects an unknown auth scheme", %{host: host} do
      assert {:error, reason} =
               Replicas.parse("postgres://logflare@#{host}:5432/logflare?auth=kerberos")

      assert reason =~ "unsupported auth=kerberos"
    end

    test "parse/1 rejects IAM aliases and missing hosts" do
      assert {:error, "IAM authentication requires an RDS hostname"} =
               Replicas.parse("postgres://logflare@database.example.com/logflare?auth=iam")

      assert {:error, "IAM authentication requires an explicit RDS hostname"} =
               Replicas.parse("postgres:///logflare?auth=iam")
    end

    test "the pool enables verified TLS and installs the per-connect IAM callback" do
      host = "logflare-proxy.proxy-abc123.eu-west-1.rds.amazonaws.com"
      entries = [Replicas.parse!("postgres://logflare@#{host}:5432/logflare?auth=iam")]

      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      ssl = Keyword.fetch!(opts, :ssl)
      assert ssl[:verify] == :verify_peer
      assert ssl[:cacerts] == :public_key.cacerts_get()

      assert {Replicas, :iam_configure, [_primary_configure]} =
               Keyword.fetch!(opts, :configure)

      refute Keyword.has_key?(opts, :auth)
    end

    test "IAM authentication rejects disabled and invalid TLS" do
      host = "logflare-proxy.proxy-abc123.eu-west-1.rds.amazonaws.com"

      for uri <- [
            "postgres://logflare@#{host}/logflare?auth=iam&ssl=false",
            "postgres://logflare@#{host}/logflare?auth=iam&ssl=invalid"
          ] do
        entries = [Replicas.parse!(uri)]

        assert_raise ArgumentError, ~r/IAM authentication requires/, fn ->
          Replicas.start_link(entries: entries)
        end
      end
    end

    test "direct RDS IAM authentication uses the configured RDS CA bundle" do
      host = "logflare.abc123.eu-west-1.rds.amazonaws.com"
      {path, certificate} = write_ca_bundle!()
      previous_path = Application.fetch_env(:logflare, :rds_ca_cert_path)
      Application.put_env(:logflare, :rds_ca_cert_path, path)
      on_exit(fn -> restore_application_env(:logflare, :rds_ca_cert_path, previous_path) end)

      entries = [Replicas.parse!("postgres://logflare@#{host}/logflare?auth=iam")]
      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      ssl = Keyword.fetch!(opts, :ssl)
      system_cacerts = :public_key.cacerts_get()
      assert ssl[:verify] == :verify_peer
      assert length(ssl[:cacerts]) == length(system_cacerts) + 1

      assert Enum.count(ssl[:cacerts], &(&1 == certificate)) ==
               Enum.count(system_cacerts, &(&1 == certificate)) + 1

      refute Keyword.has_key?(opts, :rds_ca_cert_path)
    end

    test "an IAM proxy uses system roots instead of primary SSL options" do
      put_repo_config(ssl: [cacertfile: "/primary.pem"])

      entries = [
        Replicas.parse!(
          "postgres://logflare@logflare.proxy-abc123.eu-west-1.rds.amazonaws.com/logflare?auth=iam"
        )
      ]

      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}
      ssl = Keyword.fetch!(opts, :ssl)
      assert ssl[:verify] == :verify_peer
      assert ssl[:cacerts] == :public_key.cacerts_get()
      refute Keyword.has_key?(ssl, :cacertfile)
    end

    test "rds_auth_token/3 signs for the region in the hostname", %{host: host} do
      token = Replicas.rds_auth_token(host, 5432, "logflare")

      assert String.starts_with?(token, "#{host}:5432/?")
      assert token =~ "Action=connect"
      assert token =~ "DBUser=logflare"
      assert token =~ "X-Amz-Signature="
      assert token =~ "X-Amz-Expires=900"
      assert token =~ "eu-west-1%2Frds-db"
    end

    test "rds_auth_token/3 signs temporary environment credentials and normalizes hostnames", %{
      host: host
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

      token = Replicas.rds_auth_token(String.upcase(host), 5432, "logflare")

      assert String.starts_with?(token, "#{host}:5432/?")
      assert token =~ "X-Amz-Security-Token=session-token"
    end

    test "rds_auth_token/3 preserves a session token resolved by ExAws", %{host: host} do
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

      token = Replicas.rds_auth_token(host, 5432, "logflare")
      assert token =~ "X-Amz-Security-Token=provider-session-token"
      refute token =~ "unrelated-session-token"
    end

    test "rds_auth_token/3 accepts China RDS hostnames" do
      token =
        Replicas.rds_auth_token(
          "logflare.abc123.cn-north-1.rds.amazonaws.com.cn",
          5432,
          "logflare"
        )

      assert token =~ "cn-north-1%2Frds-db"
    end

    test "direct RDS IAM authentication requires the matching partition CA bundle" do
      previous_path = Application.fetch_env(:logflare, :rds_ca_cert_path)
      Application.delete_env(:logflare, :rds_ca_cert_path)
      on_exit(fn -> restore_application_env(:logflare, :rds_ca_cert_path, previous_path) end)

      for host <- [
            "logflare.abc123.cn-north-1.rds.amazonaws.com.cn",
            "logflare.abc123.us-gov-west-1.rds.amazonaws.com"
          ] do
        entries = [Replicas.parse!("postgres://logflare@#{host}/logflare?auth=iam")]

        assert_raise ArgumentError, ~r/requires RDS_CA_CERT_PATH/, fn ->
          Replicas.start_link(entries: entries)
        end
      end
    end

    test "direct RDS IAM authentication rejects an empty CA bundle" do
      path =
        Path.join(
          System.tmp_dir!(),
          "logflare-empty-rds-ca-#{System.unique_integer([:positive])}.pem"
        )

      File.write!(path, "")
      on_exit(fn -> File.rm(path) end)

      previous_path = Application.fetch_env(:logflare, :rds_ca_cert_path)
      Application.put_env(:logflare, :rds_ca_cert_path, path)
      on_exit(fn -> restore_application_env(:logflare, :rds_ca_cert_path, previous_path) end)

      entries = [
        Replicas.parse!(
          "postgres://logflare@logflare.abc123.eu-west-1.rds.amazonaws.com/logflare?auth=iam"
        )
      ]

      assert_raise ArgumentError, ~r/contains no certificates/, fn ->
        Replicas.start_link(entries: entries)
      end
    end

    test "iam_configure/1 replaces the password with a fresh token", %{host: host} do
      opts = [hostname: host, port: 5432, username: "logflare", password: "stale"]

      configured = Replicas.iam_configure(opts)

      refute configured[:password] == "stale"
      assert configured[:password] =~ "X-Amz-Signature="
      assert configured[:hostname] == host
    end

    test "iam_configure/2 preserves both inherited configure callback shapes", %{host: host} do
      opts = [hostname: host, port: 5432, username: "original", password: "stale"]

      callbacks = [
        {fn opts -> Keyword.put(opts, :username, "from_fun") end, "from_fun"},
        {{__MODULE__, :configure_username, ["from_mfa"]}, "from_mfa"}
      ]

      for {callback, expected_username} <- callbacks do
        configured = Replicas.iam_configure(opts, callback)

        assert configured[:username] == expected_username
        assert configured[:password] =~ "DBUser=#{expected_username}"
      end
    end

    test "rds_auth_token/3 rejects a non-RDS host even when AWS_REGION is set" do
      previous_region = System.get_env("AWS_REGION")
      System.put_env("AWS_REGION", "eu-west-1")
      on_exit(fn -> restore_system_env("AWS_REGION", previous_region) end)

      assert_raise RuntimeError,
                   ~r/IAM authentication requires an RDS hostname/,
                   fn ->
                     Replicas.rds_auth_token("127.0.0.1", 5432, "logflare")
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

  defp put_repo_config(overrides) do
    previous_config = Application.fetch_env(:logflare, Repo)
    config = Application.get_env(:logflare, Repo, [])
    Application.put_env(:logflare, Repo, Keyword.merge(config, overrides))
    on_exit(fn -> restore_application_env(:logflare, Repo, previous_config) end)
  end

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
