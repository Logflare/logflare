defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica entries, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Each `LOGFLARE_READ_REPLICAS` entry is either a bare hostname or a Postgres URI
  (`postgres://user:pass@host:port/database?ssl=true&pool_size=5`). In both
  cases, only the parts explicitly given override the primary `Logflare.Repo`
  config - anything not specified (host, port, database, credentials, ssl, ...) is
  inherited from the primary's `DB_*` settings, without ever baking the primary's
  actual values into the parsed result. URIs are parsed and validated entirely by
  `Ecto.Repo.Supervisor.parse_url/1`.

  Replica pools are identified by a key derived from the entry (host/port/database
  plus a `:erlang.phash2/2` hash of the raw entry, to avoid collisions between
  entries that only differ by credentials or query params) that never contains
  credentials, so it is safe to include in log or error messages.
  Callers can temporarily redirect Ecto queries to a replica for the duration of
  a function call using `apply_with_replica/3` on `Logflare.Repo`, which swaps
  the dynamic repo and restores it afterwards.

  Every replica connection opens its session read-only. An entry may also carry
  `auth=iam` in place of a password, authenticating with a freshly minted RDS IAM
  token instead; AWS requires TLS for that, so pair it with `ssl=true`.
  """

  @registry __MODULE__.Registry

  # A physical standby rejects a stray write on its own; a logical subscriber accepts it, and
  # the divergence only surfaces later as a conflict that stalls replication.
  @read_only_statement "SET default_transaction_read_only = on"

  # RDS caps IAM authentication tokens at 15 minutes. The exact value matters less than the
  # fact that it expires at all: that is why the token is minted per connection, below,
  # rather than once when the pool starts.
  @iam_token_expiry_seconds 900

  # `<name>.<region>.rds.amazonaws.com`, covering both cluster and proxy endpoints.
  @rds_hostname ~r/\.([a-z]{2}(?:-[a-z]+)+-\d+)\.rds\.amazonaws\.com$/

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
            ] ++ resolve_auth(resolve_ssl(config), primary_configure)

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
  Runs the primary's own `:after_connect`, then marks the session read-only.

  A replica's config is the primary's with the entry's overrides applied, so setting
  `:after_connect` here would otherwise drop the primary's - the one that sets `search_path`
  from `DB_SCHEMA`. It runs first, so a hook that needs to write still can.
  """
  @spec after_connect(pid(), {module(), atom(), [term()]} | (pid() -> any()) | nil) ::
          Postgrex.Result.t()
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

  Wired in as `:configure`, which DBConnection runs before *every* connect attempt. That is
  the point rather than an implementation detail: a token lasts 15 minutes, so one minted
  when the pool started would leave it unable to reconnect after that window.
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
  Builds an RDS IAM authentication token: a SigV4-presigned URL with its scheme stripped,
  used in place of a password.
  """
  @spec rds_auth_token(String.t(), non_neg_integer(), String.t()) :: String.t()
  def rds_auth_token(hostname, port, username) do
    # `:rds` only selects credential/region defaults; ExAws has no `:rds-db` service and
    # raises building a host for one. The SigV4 *signing* service is passed separately below,
    # and that is the one that has to read `rds-db`.
    config = ExAws.Config.new(:rds, region: aws_region!(hostname))

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

  # The token is signed for the region its endpoint lives in, which the hostname encodes.
  # AWS validates that signed host, so aliases and tunnel endpoints cannot be substituted.
  defp aws_region!(hostname) do
    case Regex.run(@rds_hostname, hostname || "") do
      [_, region] ->
        region

      nil ->
        raise "cannot determine the AWS region for read replica #{inspect(hostname)}: " <>
                "IAM authentication requires an *.<region>.rds.amazonaws.com host"
    end
  end

  # `auth=iam` carries no password, so one is minted per connection instead. The key is
  # dropped rather than passed through: Postgrex has no `:auth` option. The primary's
  # configure hook runs first so unrelated dynamic connection options remain inherited.
  defp resolve_auth(config, primary_configure) do
    case Keyword.pop(config, :auth) do
      {:iam, rest} ->
        Keyword.put(rest, :configure, {__MODULE__, :iam_configure, [primary_configure]})

      {nil, rest} ->
        rest
    end
  end

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
      :error -> {:ok, config}
      {:ok, "iam"} -> {:ok, Keyword.put(config, :auth, :iam)}
      {:ok, other} -> {:error, ~s(unsupported auth=#{other}, expected "iam")}
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
      opts when is_list(opts) ->
        case :inet.parse_address(String.to_charlist(hostname || "")) do
          {:ok, _ip} -> Keyword.put(opts, :server_name_indication, :disable)
          {:error, _} -> opts
        end

      _ ->
        true
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
