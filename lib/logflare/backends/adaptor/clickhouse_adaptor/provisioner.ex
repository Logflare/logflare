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
  alias Logflare.Source

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
  def init({%Source{} = source, %Backend{} = backend} = args) do
    state = %__MODULE__{
      source: %Source{id: source.id, token: source.token},
      backend: %Backend{id: backend.id}
    }

    Process.flag(:trap_exit, true)

    with :ok <- ClickhouseAdaptor.test_connection(source, backend),
         :ok <- ClickhouseAdaptor.provision_all(args) do
      {:ok, state, {:continue, :close_process}}
    end
  end

  @doc false
  @impl GenServer
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
