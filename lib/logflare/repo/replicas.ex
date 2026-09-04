defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica entries, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Each `LOGFLARE_READ_REPLICAS` entry is either a bare hostname or a Postgres URI
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

  Every replica connection opens its session read-only.
  """

  @registry __MODULE__.Registry

  # Physical standbys already reject writes. Logical subscribers do not, so a
  # read-only default turns accidental writes into immediate errors.
  @read_only_statement "SET default_transaction_read_only = on"

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

      replicas =
        Enum.map(entries, fn {key, config} ->
          config =
            [
              name: {:via, Registry, {@registry, key}},
              after_connect: {__MODULE__, :after_connect, [primary_after_connect]}
            ] ++ resolve_ssl(config)

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

      {:ok, {build_key(config), config}}
    rescue
      e in Ecto.InvalidURLError -> {:error, redact(e.message, uri.userinfo)}
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
