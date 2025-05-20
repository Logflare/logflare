defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Supervisor do
  @moduledoc """
  Supervision tree for the Clickhouse Adaptor
  """

  use Supervisor

  alias Logflare.Backends.Backend

  @connection_sup __MODULE__.Connections
  @backend_registry Logflare.Backends.BackendRegistry

  @default_receive_timeout 10_000

  @doc false
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc false
  def init(_args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @connection_sup}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Returns count of Clickhouse `DbConnection` processes handled by this supervisor.
  """
  @spec ch_connection_count() :: non_neg_integer()
  def ch_connection_count() do
    Supervisor.count_children(@connection_sup) |> length()
  end

  @doc """
  Finds and returns a Clickhouse `DbConnection` based on a provided `Backend` or attempts to create a new one.

  These connection pools be used directly by the `Ch` library to run database transactions.
  """
  @spec find_or_create_ch_connection(Backend.t()) ::
          {:ok, pid()} | DynamicSupervisor.on_start_child()
  def find_or_create_ch_connection(%Backend{id: id} = backend) do
    case Registry.lookup(@backend_registry, build_key(id)) do
      [{pid, _meta}] ->
        {:ok, pid}

      [] ->
        start_ch_connection(backend)
    end
  end

  @doc """
  Boolean indication if a Clickhouse `DbConnection` exists for a particular `Backend` or not.
  """
  @spec ch_connection_exists?(Backend.t()) :: boolean()
  def ch_connection_exists?(%Backend{id: id}) do
    case Registry.lookup(@backend_registry, build_key(id)) do
      [] -> false
      _ -> true
    end
  end

  @doc """
  Returns a list of known `Backend` ids that have connections managed by this supervisor.
  """
  @spec backend_ids() :: [non_neg_integer()]
  def backend_ids() do
    Supervisor.which_children(@connection_sup)
    |> Enum.map(fn {_, connection_pid, _, _} ->
      Registry.keys(@backend_registry, connection_pid)
      |> List.first()
      |> elem(1)
    end)
    |> Enum.sort()
  end

  @spec start_ch_connection(Backend.t()) :: DynamicSupervisor.on_start_child()
  defp start_ch_connection(%Backend{} = backend) do
    # i'm doing something dumb here. need to fix this.
    config = Map.get(backend, :config) || Map.get(backend, :config_encrypted)

    default_pool_size = Application.fetch_env!(:logflare, :clickhouse_backend_adapter)[:pool_size]
    url = Map.get(config, :url) || Map.get(config, "url")
    port = Map.get(config, :port) || Map.get(config, "port")
    port = String.to_integer(port)

    # ensure things parse correctly on the instance URL
    # handle this smoother. maybe raise instead.
    {:ok, {scheme, hostname}} = extract_scheme_and_hostname(url)

    # need to clean this nonsense up as the config should likely
    # not be showing up as string keys in reality
    ch_opts = [
      name: via(backend),
      scheme: scheme,
      hostname: hostname,
      port: port,
      database: Map.get(config, :database) || Map.get(config, "database"),
      username: Map.get(config, :username) || Map.get(config, "username"),
      password: Map.get(config, :password) || Map.get(config, "password"),
      pool_size: Map.get(config, :pool_size, default_pool_size),
      settings: [],
      timeout: @default_receive_timeout
    ]

    DynamicSupervisor.start_child(@connection_sup, Ch.child_spec(ch_opts))
  end

  @spec via(Backend.t()) :: {:via, module(), term()}
  defp via(%Backend{id: id}), do: {:via, Registry, {@backend_registry, build_key(id)}}

  @spec build_key(term()) :: {module(), term()}
  defp build_key(id), do: {__MODULE__, id}

  @spec extract_scheme_and_hostname(String.t()) ::
          {:ok, {String.t(), String.t()}} | {:error, String.t()}
  defp extract_scheme_and_hostname(url) when is_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: hostname}} when scheme in ~w(http https) ->
        {:ok, {scheme, hostname}}

      {:ok, %URI{}} ->
        {:error, "Unable to extract scheme and hostname from URL."}

      {:error, _err_msg} = error ->
        error
    end
  end

  defp extract_scheme_and_hostname(_url), do: {:error, "Unexpected URL value provided."}
end
