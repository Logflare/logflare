defmodule Logflare.Repo.Replicas do
  @moduledoc """
  Manages a pool of PostgreSQL read replica connections for `Logflare.Repo`.

  When started with one or more replica entries, this module supervises a separate
  `Logflare.Repo` connection pool for each replica, registered under a local
  `Registry`. If no replicas are configured, the supervisor is skipped entirely.

  Each `LOGFLARE_READ_REPLICAS` entry is either a bare hostname or a Postgres URI
  (`postgres://user:pass@host:port/database?ssl=true&pool_size=5`). In both
  cases, only the parts explicitly given override the primary `Logflare.Repo`
  config - anything not specified (port, database, credentials, ssl, ...) is
  inherited from the primary's `DB_*` settings. URIs are parsed via
  `Ecto.Repo.Supervisor.parse_url/1`.

  Replica pools are identified by a key derived from the entry that never
  contains credentials, so it is safe to include in log or error messages.
  Callers can temporarily redirect Ecto queries to a replica for the duration of
  a function call using `apply_with_replica/3` on `Logflare.Repo`, which swaps
  the dynamic repo and restores it afterwards.
  """

  @registry __MODULE__.Registry
  @allowed_query_keys ~w(ssl pool_size)a
  @placeholder_database "__logflare_replica__"

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
      replicas =
        Enum.map(entries, fn {key, config} ->
          config = [name: {:via, Registry, {@registry, key}}] ++ resolve_ssl(config)
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
      {:ok, {entry, maybe_put_socket_options([hostname: entry], entry)}}
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

    with :ok <- validate_scheme(uri.scheme),
         :ok <- validate_host(uri.host) do
      has_database? = uri.path not in [nil, "", "/"]

      parseable_url =
        if has_database?, do: uri, else: %{uri | path: "/#{@placeholder_database}"}

      try do
        parsed = Ecto.Repo.Supervisor.parse_url(URI.to_string(parseable_url))
        config = parsed |> Keyword.delete(:scheme) |> maybe_delete_database(has_database?)

        with :ok <- validate_query_keys(config),
             :ok <- validate_pool_size(config[:pool_size]) do
          config = maybe_put_socket_options(config, uri.host)
          {:ok, {build_key(uri.host, config[:port], config[:database]), config}}
        end
      rescue
        e in Ecto.InvalidURLError -> {:error, e.message}
      end
    end
  end

  defp maybe_delete_database(config, true), do: config
  defp maybe_delete_database(config, false), do: Keyword.delete(config, :database)

  defp validate_scheme(scheme) when scheme in ["postgres", "postgresql"], do: :ok
  defp validate_scheme(scheme), do: {:error, "unsupported scheme #{inspect(scheme)}"}

  defp validate_host(host) when is_binary(host) and host != "", do: :ok
  defp validate_host(_host), do: {:error, "missing host"}

  defp validate_query_keys(config) do
    known_keys = [:hostname, :username, :password, :database, :port | @allowed_query_keys]

    case Enum.find(Keyword.keys(config), &(&1 not in known_keys)) do
      nil -> :ok
      unknown -> {:error, "unknown query parameter #{inspect(unknown)}"}
    end
  end

  defp validate_pool_size(nil), do: :ok
  defp validate_pool_size(pool_size) when pool_size > 0, do: :ok

  defp validate_pool_size(pool_size),
    do: {:error, "invalid pool_size query parameter #{pool_size}"}

  defp maybe_put_socket_options(config, host) do
    case Logflare.Utils.ip_version(host) do
      version when version in [:inet, :inet6] -> Keyword.put(config, :socket_options, [version])
      _ -> config
    end
  end

  defp build_key(host, port, database) do
    host
    |> then(fn key -> if port, do: "#{key}:#{port}", else: key end)
    |> then(fn key -> if database, do: "#{key}/#{database}", else: key end)
  end

  defp redact(entry) do
    if String.contains?(entry, "://") do
      uri = URI.parse(entry)
      URI.to_string(%{uri | userinfo: if(uri.userinfo, do: "REDACTED")})
    else
      entry
    end
  end

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
