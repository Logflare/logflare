defmodule Logflare.TestSupport.FakeConsolidatedAdaptor do
  @moduledoc false

  use GenServer

  alias Logflare.Backends
  alias Logflare.Backends.Backend

  def child_spec(%Backend{} = backend) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [backend]},
      type: :supervisor
    }
  end

  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @impl GenServer
  def init(backend) do
    {:ok, %{backend: backend}}
  end
end
