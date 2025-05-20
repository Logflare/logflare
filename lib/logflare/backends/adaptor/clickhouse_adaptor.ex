defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor do
  @moduledoc """
  ClickHouse backend adaptor that relies on the `:ch` library.
  """

  use GenServer
  use TypedStruct
  require Logger

  alias Ecto.Changeset

  typedstruct enforce: true do
    field(:url, String.t())
    field(:username, String.t())
    field(:password, String.t())
    field(:database, String.t(), default: "default")
    field(:port, non_neg_integer(), default: 8443)
    field(:pool_size, non_neg_integer(), default: 1)
  end

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    GenServer.start_link(__MODULE__, {source, backend},
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl GenServer
  def init({_source, backend}) do
    # args = %{
    #   config: backend.config,
    #   source: source,
    #   backend: backend
    # }

    {:ok, connection_pid} = __MODULE__.Supervisor.start_child_connection(backend)

    IO.inspect(connection_pid, label: "Connection pid for backend #{backend.id}")

    {:ok, %{}}
  end
end
