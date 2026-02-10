defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.Provisioner do
  @moduledoc """
  Short-lived process that kicks off when spinning up a `ClickHouseAdaptor`.
  Used to provision resources (tables/views) within ClickHouse.

  It is assumed that the `ClickHouseAdaptor` is running _before_ starting this process.
  """

  use GenServer

  require Logger

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend

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

    {:ok, backend, {:continue, :test_connection}}
  end

  @doc false
  @impl GenServer
  def handle_continue(:test_connection, %Backend{} = backend) do
    with :ok <- ClickHouseAdaptor.test_connection(backend) do
      {:noreply, backend, {:continue, :provision_tables}}
    else
      {:error, reason} = error ->
        Logger.error("ClickHouse test connection failed",
          backend_id: backend.id,
          error_string: inspect(reason)
        )

        {:stop, error, backend}
    end
  end

  def handle_continue(:provision_tables, %Backend{} = backend) do
    case ClickHouseAdaptor.provision_ingest_tables(backend) do
      :ok ->
        {:noreply, backend, {:continue, :close_process}}

      {:error, reason} = error ->
        Logger.error("ClickHouse provisioning failed",
          backend_id: backend.id,
          error_string: inspect(reason)
        )

        {:stop, error, backend}
    end
  end

  def handle_continue(:close_process, state), do: {:stop, :normal, state}

  @doc false
  @impl GenServer
  def terminate(:normal, _state), do: :ok

  def terminate(reason, %Backend{} = backend) do
    Logger.warning("Terminating #{__MODULE__}: '#{inspect(reason)}'",
      backend_id: backend.id
    )

    :ok
  end
end
