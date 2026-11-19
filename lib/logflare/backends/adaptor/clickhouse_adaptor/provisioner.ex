defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner do
  @moduledoc """
  Provisions the ingest tables of a `ClickHouseAdaptor` backend and exits normally once
  they exist, or once the backend is deleted.

  Every attempt reads the backend's committed config from the primary database, since
  `Logflare.Backends.Cache` may lag behind a config change or a just-created backend. A
  failed read, connection test or table creation is retried with exponential backoff,
  capped at one minute, for as long as the process runs, so a config fixed between
  attempts is used by the next one.

  It is assumed that the `ClickHouseAdaptor` is running _before_ starting this process.
  """

  use GenServer

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Repo

  @initial_retry_ms :timer.seconds(1)
  @max_retry_ms :timer.minutes(1)

  @doc false
  @spec child_spec(pos_integer()) :: Supervisor.child_spec()
  def child_spec(backend_id) do
    %{
      id: __MODULE__,
      restart: :transient,
      start: {__MODULE__, :start_link, [backend_id]}
    }
  end

  @doc false
  @spec start_link(pos_integer()) :: GenServer.on_start()
  def start_link(backend_id) when is_integer(backend_id) do
    GenServer.start_link(__MODULE__, backend_id)
  end

  @doc false
  @impl GenServer
  def init(backend_id) do
    Process.flag(:trap_exit, true)

    state = %{backend_id: backend_id, backend: nil, retry_ms: initial_retry_ms()}
    {:ok, state, {:continue, :load_backend}}
  end

  @doc false
  @impl GenServer
  def handle_continue(:load_backend, %{backend_id: backend_id} = state) do
    case fetch_backend(backend_id) do
      {:ok, %Backend{} = backend} ->
        {:noreply, %{state | backend: backend}, {:continue, :test_connection}}

      {:ok, nil} ->
        {:stop, :normal, state}

      {:error, reason} ->
        schedule_retry("ClickHouse backend lookup failed", reason, state)
    end
  end

  def handle_continue(:test_connection, %{backend: backend} = state) do
    case ClickHouseAdaptor.test_ingest_connection(backend) do
      :ok -> {:noreply, state, {:continue, :provision_tables}}
      {:error, reason} -> schedule_retry("ClickHouse test connection failed", reason, state)
    end
  end

  def handle_continue(:provision_tables, %{backend: backend} = state) do
    case ClickHouseAdaptor.provision_ingest_tables(backend) do
      :ok -> {:stop, :normal, state}
      {:error, reason} -> schedule_retry("ClickHouse provisioning failed", reason, state)
    end
  end

  @doc false
  @impl GenServer
  def handle_info(:retry, state), do: {:noreply, state, {:continue, :load_backend}}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @spec fetch_backend(pos_integer()) :: {:ok, Backend.t() | nil} | {:error, Exception.t()}
  defp fetch_backend(backend_id) do
    {:ok, Repo.with_primary(fn -> Backends.get_backend(backend_id) end)}
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] -> {:error, error}
  end

  @spec schedule_retry(String.t(), term(), map()) :: {:noreply, map()}
  defp schedule_retry(message, reason, %{retry_ms: retry_ms} = state) do
    Logger.warning(message,
      backend_id: state.backend_id,
      host: config_host(state.backend),
      error_string: inspect(reason),
      retry_in_ms: retry_ms
    )

    Process.send_after(self(), :retry, retry_ms)
    {:noreply, %{state | retry_ms: min(retry_ms * 2, max_retry_ms())}}
  end

  @spec config_host(Backend.t() | nil) :: String.t() | nil
  defp config_host(%Backend{config: %{url: url}}) when is_binary(url), do: URI.parse(url).host
  defp config_host(_backend), do: nil

  defp initial_retry_ms,
    do: Application.get_env(:logflare, __MODULE__)[:initial_retry_ms] || @initial_retry_ms

  defp max_retry_ms,
    do: Application.get_env(:logflare, __MODULE__)[:max_retry_ms] || @max_retry_ms

  @doc false
  @impl GenServer
  def terminate(:normal, _state), do: :ok
  def terminate(:shutdown, _state), do: :ok
  def terminate({:shutdown, _reason}, _state), do: :ok

  def terminate(reason, %{backend_id: backend_id}) do
    Logger.warning("Terminating #{__MODULE__}: '#{inspect(reason)}'", backend_id: backend_id)

    :ok
  end
end
