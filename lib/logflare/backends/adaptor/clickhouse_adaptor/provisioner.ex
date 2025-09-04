defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.Provisioner do
  @moduledoc """
  Short-lived process that kicks off when spinning up a `ClickhouseAdaptor`.
  Used to provision resources (tables/views) within ClickHouse.

  It is assumed that the `ClickhouseAdaptor` is running _before_ starting this process.
  """

  use GenServer
  use TypedStruct

  require Logger

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source

  typedstruct do
    field(:source, Source.t())
    field(:backend, Backend.t())
  end

  @doc false
  def child_spec(arg) do
    %{
      id: __MODULE__,
      restart: :transient,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @doc false
  @spec start_link(ClickhouseAdaptor.source_backend_tuple()) :: GenServer.on_start()
  def start_link({%Source{}, %Backend{}} = args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc false
  @impl GenServer
  def init({%Source{} = source, %Backend{} = backend}) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      source: source,
      backend: backend
    }

    {:ok, state, {:continue, :test_connection}}
  end

  @doc false
  @impl GenServer
  def handle_continue(
        :test_connection,
        %__MODULE__{source: %Source{} = source, backend: %Backend{} = backend} = state
      ) do
    with :ok <- ClickhouseAdaptor.test_connection({source, backend}) do
      {:noreply, state, {:continue, :provision_all}}
    else
      {:error, reason} = error ->
        Logger.error("ClickHouse test connection failed",
          source_token: source.token,
          backend_id: backend.id,
          error_string: inspect(reason)
        )

        {:stop, error, state}
    end
  end

  def handle_continue(
        :provision_all,
        %__MODULE__{source: %Source{} = source, backend: %Backend{} = backend} = state
      ) do
    with :ok <- ClickhouseAdaptor.provision_all({source, backend}) do
      {:noreply, state, {:continue, :close_process}}
    else
      {:error, reason} = error ->
        Logger.error("ClickHouse provisioning failed",
          source_token: source.token,
          backend_id: backend.id,
          error_string: inspect(reason)
        )

        {:stop, error, state}
    end
  end

  def handle_continue(:close_process, state), do: {:stop, :normal, state}

  @doc false
  @impl GenServer
  def terminate(:normal, state), do: {:noreply, state}

  def terminate(reason, %__MODULE__{} = state) do
    Logger.warning("Terminating #{__MODULE__}: '#{inspect(reason)}'",
      source_token: state.source.token,
      backend_id: state.backend.id
    )

    {:noreply, state}
  end
end
