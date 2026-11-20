defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner do
  @moduledoc """
  Provisions the ingest tables of a `ClickHouseAdaptor` backend and exits normally once
  they exist.

  A failed connection test or table creation is retried with exponential backoff, capped
  at one minute, for as long as the process runs.

  It is assumed that the `ClickHouseAdaptor` is running _before_ starting this process.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend

  @initial_retry_ms :timer.seconds(1)
  @max_retry_ms :timer.minutes(1)

  @doc false
  def child_spec(%Backend{} = backend) do
    %{
      id: __MODULE__,
      restart: :transient,
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc false
  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend)
  end

  @doc false
  @impl GenServer
  def init(%Backend{} = backend) do
    Process.flag(:trap_exit, true)

    {:ok, %{backend: backend, retry_ms: initial_retry_ms()}, {:continue, :test_connection}}
  end

  @doc false
  @impl GenServer
  def handle_continue(:test_connection, %{backend: backend} = state) do
    case ClickHouseAdaptor.test_ingest_connection(backend) do
      :ok -> {:noreply, state, {:continue, :provision_tables}}
      {:error, reason} -> schedule_retry("ClickHouse test connection failed", reason, state)
    end
  end

  def handle_continue(:provision_tables, %{backend: backend} = state) do
    case ClickHouseAdaptor.provision_ingest_tables(backend) do
      :ok -> {:noreply, state, {:continue, :close_process}}
      {:error, reason} -> schedule_retry("ClickHouse provisioning failed", reason, state)
    end
  end

  def handle_continue(:close_process, state), do: {:stop, :normal, state}

  @doc false
  @impl GenServer
  def handle_info(:retry, state), do: {:noreply, state, {:continue, :test_connection}}

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @spec schedule_retry(String.t(), term(), map()) :: {:noreply, map()}
  defp schedule_retry(message, reason, %{backend: backend, retry_ms: retry_ms} = state) do
    Logger.warning(message,
      backend_id: backend.id,
      host: config_host(backend),
      error_string: inspect(reason),
      retry_in_ms: retry_ms
    )

    Process.send_after(self(), :retry, retry_ms)
    {:noreply, %{state | retry_ms: min(retry_ms * 2, max_retry_ms())}}
  end

  @spec config_host(Backend.t()) :: String.t() | nil
  defp config_host(%Backend{config: %{url: url}}) when is_binary(url), do: URI.parse(url).host
  defp config_host(_backend), do: nil

  @spec initial_retry_ms() :: pos_integer()
  defp initial_retry_ms,
    do: Application.get_env(:logflare, __MODULE__)[:initial_retry_ms] || @initial_retry_ms

  @spec max_retry_ms() :: pos_integer()
  defp max_retry_ms,
    do: Application.get_env(:logflare, __MODULE__)[:max_retry_ms] || @max_retry_ms

  @doc false
  @impl GenServer
  def terminate(:normal, _state), do: :ok

  def terminate(reason, %{backend: backend}) do
    Logger.warning("Terminating #{__MODULE__}: '#{inspect(reason)}'",
      backend_id: backend.id
    )

    :ok
  end
end
