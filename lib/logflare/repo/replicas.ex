defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica entries, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Each `LOGFLARE_READ_REPLICAS` entry is either a bare host name or IP literal, or a Postgres URI
  (`postgres://user:pass@host:port/database?ssl=true&pool_size=5`). Supplied
  connection settings override the primary `Logflare.Repo` configuration; omitted
  settings inherit the primary's `DB_*` values without copying them into the parsed
  result. IP literal hosts also derive the matching socket address family. URIs are
  parsed and validated by `Ecto.Repo.Supervisor.parse_url/1`.

  Replica pools are identified by the parsed hostname plus an `:erlang.phash2/2`
  hash of the parsed configuration. The key never contains credentials, so it is
  safe to include in log or error messages.

  Callers can temporarily redirect Ecto queries to a replica for the duration of
  a function call using `apply_with_replica/3` on `Logflare.Repo`, which swaps
  the dynamic repo and restores it afterwards.

  Every replica connection opens its session read-only. An entry may use
  `auth=iam` instead of a password; IAM connections always use verified TLS.
  """

  @registry __MODULE__.Registry

  # Physical standbys already reject writes. Logical subscribers do not, so a
  # read-only default turns accidental writes into immediate errors.
  @read_only_statement "SET default_transaction_read_only = on"

  @iam_token_expiry_seconds 900
  @default_rds_ca_cert_path "/etc/ssl/certs/aws-rds-global-bundle.pem"
  @rds_hostname ~r/\.([a-z]{2}(?:-[a-z]+)+-\d+)\.rds\.amazonaws\.com(?:\.cn)?$/i

  def child_spec(options) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [options]},
      type: :supervisor
    }
  end

  def start_link(options) do
    entries = Keyword.fetch!(options, :entries)
    ensure_unique_keys!(entries)

    if entries == [] do
      :ignore
    else
      primary_after_connect = Logflare.Repo.config()[:after_connect]
      primary_configure = Logflare.Repo.config()[:configure]

      replicas =
        Enum.map(entries, fn {key, config} ->
          config =
            [
              name: {:via, Registry, {@registry, key}},
              after_connect: {__MODULE__, :after_connect, [primary_after_connect]}
            ] ++ resolve_ssl(resolve_auth(config, primary_configure))

          Supervisor.child_spec({Logflare.Repo, config}, id: key)
        end)

      children = [
        {Registry, name: @registry, keys: :unique}
        | replicas
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
    end
  end

  @doc """
  Looks up the PID of the replica connection pool for the given key.
  Raises if no such replica is found.
  """
  def lookup!(key) do
    case Registry.lookup(@registry, key) do
      [{pid, _}] -> pid
      [] -> raise "unknown replica: #{inspect(key)}"
    end
  end

  @doc """
  Runs the primary repository's `:after_connect` hook, then makes the replica session
  read-only.

  Replica pools inherit the primary configuration. Replacing `:after_connect` without
  composition would discard settings such as the `search_path` configured by `DB_SCHEMA`.
  The inherited hook runs first so it can perform any required writes.
  """
  @spec after_connect(
          DBConnection.conn(),
          {module(), atom(), [term()]} | (DBConnection.t() -> any()) | nil
        ) :: Postgrex.Result.t()
  def after_connect(conn, primary_after_connect) do
    run_after_connect(conn, primary_after_connect)
    Postgrex.query!(conn, @read_only_statement, [])
  end

  defp run_after_connect(_conn, nil), do: :ok

  defp run_after_connect(conn, {module, function, args}),
    do: apply(module, function, [conn | args])

  defp run_after_connect(conn, fun) when is_function(fun, 1), do: fun.(conn)

  @doc """
  Replaces the password with a freshly minted RDS IAM authentication token.

  DBConnection calls `:configure` before every connect attempt.
  """
  @spec iam_configure(keyword()) :: keyword()
  def iam_configure(opts), do: iam_configure(opts, nil)

  @doc false
  @spec iam_configure(
          keyword(),
          {module(), atom(), [term()]} | (keyword() -> keyword()) | nil
        ) :: keyword()
  def iam_configure(opts, primary_configure) do
    opts = run_configure(opts, primary_configure)
    hostname = Keyword.fetch!(opts, :hostname)
    username = Keyword.fetch!(opts, :username)
    port = Keyword.get(opts, :port) || 5432

    Keyword.put(opts, :password, rds_auth_token(hostname, port, username))
  end

  defp run_configure(opts, nil), do: opts

  defp run_configure(opts, {module, function, args}),
    do: apply(module, function, [opts | args])

  defp run_configure(opts, fun) when is_function(fun, 1), do: fun.(opts)

  @doc """
  Builds an RDS IAM authentication token used in place of a password.
  """
  @spec rds_auth_token(String.t(), non_neg_integer(), String.t()) :: String.t()
  def rds_auth_token(hostname, port, username) do
    hostname = String.downcase(hostname)

    config =
      :rds
      |> ExAws.Config.new(region: aws_region!(hostname))
      |> maybe_put_env_session_token()

    {:ok, url} =
      ExAws.Auth.presigned_url(
        :get,
        "https://#{hostname}:#{port}/",
        :"rds-db",
        NaiveDateTime.to_erl(NaiveDateTime.utc_now()),
        config,
        @iam_token_expiry_seconds,
        [{"Action", "connect"}, {"DBUser", username}]
      )

    String.replace_prefix(url, "https://", "")
  end

  defp maybe_put_env_session_token(config) do
    case {
      System.get_env("AWS_ACCESS_KEY_ID"),
      System.get_env("AWS_SECRET_ACCESS_KEY"),
      System.get_env("AWS_SESSION_TOKEN")
    } do
      {access_key_id, secret_access_key, token}
      when is_binary(access_key_id) and access_key_id != "" and
             is_binary(secret_access_key) and secret_access_key != "" and
             is_binary(token) and token != "" ->
        if config.access_key_id == access_key_id and config.secret_access_key == secret_access_key,
          do: Map.put(config, :security_token, token),
          else: config

      _ ->
        config
    end
  end

  defp aws_region!(hostname) do
    case Regex.run(@rds_hostname, hostname) do
      [_, region] ->
        region

      nil ->
        raise "cannot determine the AWS region for read replica #{inspect(hostname)}: " <>
                "IAM authentication requires an RDS hostname"
    end
  end

  defp resolve_auth(config, primary_configure) do
    case Keyword.pop(config, :auth) do
      {:iam, rest} ->
        rest
        |> put_iam_ssl!()
        |> Keyword.put(:configure, {__MODULE__, :iam_configure, [primary_configure]})

      {nil, rest} ->
        rest
    end
  end

  defp put_iam_ssl!(config) do
    hostname = config |> Keyword.fetch!(:hostname) |> String.downcase()
    aws_region!(hostname)

    case Keyword.get(config, :ssl) do
      false ->
        raise ArgumentError, "IAM authentication requires TLS; remove ssl=false"

      nil ->
        Keyword.put(config, :ssl, iam_ssl_opts!(hostname))

      true ->
        Keyword.put(config, :ssl, iam_ssl_opts!(hostname))

      opts when is_list(opts) ->
        validate_iam_ssl_opts!(opts, hostname)
        Keyword.put(config, :ssl, replica_ssl_opts(opts, hostname))

      _ ->
        raise ArgumentError, "IAM authentication requires ssl=true or verified SSL options"
    end
  end

  defp validate_iam_ssl_opts!(opts, hostname) do
    if Keyword.get(opts, :verify) != :verify_peer do
      raise ArgumentError,
            "IAM authentication requires verify: :verify_peer for #{inspect(hostname)}"
    end
  end

  defp iam_ssl_opts!(hostname) do
    cacerts =
      if rds_proxy?(hostname),
        do: :public_key.cacerts_get(),
        else: system_and_rds_cacerts!(hostname)

    [
      verify: :verify_peer,
      cacerts: cacerts,
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end

  defp system_and_rds_cacerts!(hostname) do
    path = rds_ca_cert_path!(hostname)

    rds_cacerts =
      path
      |> File.read!()
      |> :public_key.pem_decode()
      |> Enum.flat_map(fn
        {:Certificate, der, _} -> [{:cert, der, :public_key.pkix_decode_cert(der, :otp)}]
        _ -> []
      end)

    if rds_cacerts == [] do
      raise ArgumentError, "RDS CA bundle #{inspect(path)} contains no certificates"
    end

    :public_key.cacerts_get() ++ rds_cacerts
  end

  defp rds_ca_cert_path!(hostname) do
    path = Application.get_env(:logflare, :rds_ca_cert_path, @default_rds_ca_cert_path)

    if custom_partition_bundle_required?(hostname) and path == @default_rds_ca_cert_path do
      raise ArgumentError,
            "IAM authentication for this AWS partition requires RDS_CA_CERT_PATH"
    end

    path
  end

  defp custom_partition_bundle_required?(hostname) do
    region = aws_region!(String.downcase(hostname))
    String.starts_with?(region, "cn-") or String.starts_with?(region, "us-gov-")
  end

  defp rds_proxy?(hostname), do: String.contains?(String.downcase(hostname), ".proxy-")

  @doc """
  Parses a single `LOGFLARE_READ_REPLICAS` entry into a `{key, config}` pair.

  `config` is a keyword list containing only the parts explicitly given in the
  entry - anything else is inherited from the primary `Logflare.Repo` config
  when the replica pool is started. `key` uniquely identifies the replica for
  `Registry` lookups and is guaranteed to never contain credentials.
  """
  @spec parse(String.t()) :: {:ok, {String.t(), keyword()}} | {:error, String.t()}
  def parse(entry) when is_binary(entry) do
    if String.contains?(entry, "://") do
      parse_uri(entry)
    else
      {:ok, {entry, maybe_put_socket_options(hostname: entry)}}
    end
  end

  @doc """
  Same as `parse/1`, but raises `ArgumentError` with a credential-redacted
  message on failure.
  """
  @spec parse!(String.t()) :: {String.t(), keyword()}
  def parse!(entry) do
    case parse(entry) do
      {:ok, result} ->
        result

      {:error, reason} ->
        raise ArgumentError, "invalid read replica #{inspect(redact(entry))}: #{reason}"
    end
  end

  defp parse_uri(entry) do
    uri = URI.parse(entry)

    try do
      config =
        case uri.path do
          v when v not in [nil, "", "/"] ->
            entry
            |> Ecto.Repo.Supervisor.parse_url()

          _ ->
            # no database set - use a placeholder to satisfy Ecto's URL parser,
            # then drop it so the primary's database is inherited instead
            URI.to_string(%{uri | path: "/placeholder"})
            |> Ecto.Repo.Supervisor.parse_url()
            |> Keyword.delete(:database)
        end
        |> Keyword.delete(:scheme)
        |> maybe_put_socket_options()

      with {:ok, config} <- normalize_auth(config) do
        {:ok, {build_key(config), config}}
      end
    rescue
      e in Ecto.InvalidURLError -> {:error, redact(e.message, uri.userinfo)}
    end
  end

  defp normalize_auth(config) do
    case Keyword.fetch(config, :auth) do
      :error ->
        {:ok, config}

      {:ok, "iam"} ->
        normalize_iam_auth(config)

      {:ok, other} ->
        {:error, ~s(unsupported auth=#{other}, expected "iam")}
    end
  end

  defp normalize_iam_auth(config) do
    case Keyword.fetch(config, :hostname) do
      {:ok, hostname} when is_binary(hostname) ->
        put_iam_hostname(config, String.downcase(hostname))

      _ ->
        {:error, "IAM authentication requires an explicit RDS hostname"}
    end
  end

  defp put_iam_hostname(config, hostname) do
    if Regex.match?(@rds_hostname, hostname) do
      {:ok, config |> Keyword.put(:auth, :iam) |> Keyword.put(:hostname, hostname)}
    else
      {:error, "IAM authentication requires an RDS hostname"}
    end
  end

  defp maybe_put_socket_options(config) do
    case Logflare.Utils.ip_version(config[:hostname]) do
      version when version in [:inet, :inet6] -> Keyword.put(config, :socket_options, [version])
      _ -> config
    end
  end

  defp build_key(config) do
    "#{config[:hostname]}-#{:erlang.phash2(config)}"
  end

  defp redact(entry) do
    if String.contains?(entry, "://") do
      uri = URI.parse(entry)
      URI.to_string(%{uri | userinfo: if(uri.userinfo, do: "REDACTED")})
    else
      entry
    end
  end

  defp redact(message, nil), do: message
  defp redact(message, userinfo), do: String.replace(message, userinfo, "REDACTED")

  defp resolve_ssl(config) do
    case Keyword.get(config, :ssl) do
      true -> Keyword.put(config, :ssl, primary_ssl_opts(config[:hostname]))
      _ -> config
    end
  end

  defp primary_ssl_opts(hostname) do
    case Keyword.get(Logflare.Repo.config(), :ssl) do
      opts when is_list(opts) -> replica_ssl_opts(opts, hostname)
      _ -> true
    end
  end

  defp replica_ssl_opts(opts, hostname) do
    opts = Keyword.delete(opts, :server_name_indication)

    case :inet.parse_address(String.to_charlist(hostname || "")) do
      {:ok, _ip} -> Keyword.put(opts, :server_name_indication, :disable)
      {:error, _} -> opts
    end
  end

  defp ensure_unique_keys!(entries) do
    duplicate =
      entries
      |> Enum.map(fn {key, _config} -> key end)
      |> Enum.frequencies()
      |> Enum.find(fn {_key, count} -> count > 1 end)

    case duplicate do
      {key, _count} -> raise ArgumentError, "duplicate read replica: #{inspect(key)}"
      nil -> :ok
    end
  end
end
