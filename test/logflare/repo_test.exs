defmodule Logflare.RepoTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo
  alias Logflare.Repo.Replicas

  defp start_read_replicas(raw_entries) do
    primary_hostname = Keyword.fetch!(Repo.config(), :hostname)
    entries = Enum.map(raw_entries, &Replicas.parse!/1)

    # sanity check that our test replicas are not the same as the primary
    for {_key, config} <- entries do
      refute config[:hostname] == primary_hostname,
             "replica hostname #{config[:hostname]} should be different from primary hostname"
    end

    # we read the replicas from env in `apply_with_replica/3`, so we need to set it there for the test
    prev_read_replicas = Application.get_env(:logflare, :read_replicas)
    Application.put_env(:logflare, :read_replicas, entries)
    on_exit(fn -> Application.put_env(:logflare, :read_replicas, prev_read_replicas) end)

    # attach repo init handler to ensure we start the replicas with the expected config
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

      # verify that after the call, we're back to the default repo
      assert Repo.get_dynamic_repo() == Repo
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

  describe "IAM authentication" do
    setup do
      # ExAws resolves credentials from app env; a real signature needs some, not valid ones.
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

    test "the pool is configured to mint a token per connection rather than carry a password" do
      host = "logflare-proxy.proxy-abc123.eu-west-1.rds.amazonaws.com"
      entries = [Replicas.parse!("postgres://logflare@#{host}:5432/logflare?auth=iam&ssl=true")]

      telemetry_ref = :telemetry_test.attach_event_handlers(self(), [[:ecto, :repo, :init]])
      on_exit(fn -> :telemetry.detach(telemetry_ref) end)

      start_supervised!({Replicas, entries: entries})

      assert_receive {[:ecto, :repo, :init], ^telemetry_ref, _, %{repo: Repo, opts: opts}}

      assert {Replicas, :iam_configure, [_primary_configure]} =
               Keyword.fetch!(opts, :configure)

      # :auth is translated, not forwarded -- Postgrex has no such option.
      refute Keyword.has_key?(opts, :auth)
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
                   ~r/IAM authentication requires an .*rds\.amazonaws\.com host/,
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
  end

  def configure_username(opts, username), do: Keyword.put(opts, :username, username)

  defp restore_application_env(app, key, {:ok, value}), do: Application.put_env(app, key, value)
  defp restore_application_env(app, key, :error), do: Application.delete_env(app, key)

  defp restore_system_env(key, nil), do: System.delete_env(key)
  defp restore_system_env(key, value), do: System.put_env(key, value)
end
