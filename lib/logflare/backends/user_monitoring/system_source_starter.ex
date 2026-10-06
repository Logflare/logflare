defmodule Logflare.Backends.UserMonitoring.SystemSourceStarter do
  @moduledoc """
  Starts the `SourceSup` of a user's system logs source outside the process that logs.

  `Logflare.Backends.UserMonitoring.log_interceptor/2` runs in the logging process, which can be a
  process inside a `SourceSup` start. A synchronous start from there can wait on the same
  `SourcesSup` partition and deadlock it. The interceptor casts the source id here instead, and
  this process runs the start. Requests for a source whose `SourceSup` is already up are skipped.
  """

  use GenServer

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Source

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Requests a start of the `SourceSup` for the source id. Returns at once.
  """
  @spec request_start(pos_integer()) :: :ok
  def request_start(source_id) when is_integer(source_id) do
    GenServer.cast(__MODULE__, {:start, source_id})
  end

  @impl GenServer
  def init(_opts), do: {:ok, %{}}

  @impl GenServer
  def handle_cast({:start, source_id}, state) do
    with false <- Backends.source_sup_started?(source_id),
         %Source{} = source <- Sources.Cache.get_by_id(source_id) do
      Backends.ensure_source_sup_started(source)
    end

    {:noreply, state}
  end
end
