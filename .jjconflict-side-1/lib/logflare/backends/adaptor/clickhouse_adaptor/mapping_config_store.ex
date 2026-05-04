defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore do
  @moduledoc """
  Global singleton GenServer that compiles and caches ClickHouse mapping configs.

  Compiled NIF references are stored in a dedicated ETS table for fast,
  concurrent reads from pipeline batchers. If the GenServer restarts,
  `init/1` recreates the table and recompiles all configs.

  Started globally from `Logflare.Backends.Supervisor`.
  """

  use GenServer

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  require Logger

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper

  @table :mapping_config_store
  @event_types [:log, :metric, :trace]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_arg) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Returns the compiled mapping reference and config ID for the given event type.

  On a cache miss (e.g. after a crash/restart race), recompiles on the fly.
  """
  @spec get_compiled(TypeDetection.event_type()) :: {:ok, reference(), String.t()}
  def get_compiled(event_type) when is_event_type(event_type) do
    case :ets.lookup(@table, event_type) do
      [{^event_type, compiled, config_id}] ->
        {:ok, compiled, config_id}

      [] ->
        Logger.warning(
          "ClickHouse mapping config cache miss for #{inspect(event_type)}, recompiling"
        )

        {compiled, config_id} = compile_and_store(event_type)
        {:ok, compiled, config_id}
    end
  end

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])

    Enum.each(@event_types, &compile_and_store/1)

    Logger.info("ClickHouse mapping configs compiled and cached", event_types: @event_types)
    {:ok, %{}}
  end

  @spec compile_and_store(TypeDetection.event_type()) :: {reference(), String.t()}
  defp compile_and_store(event_type) do
    compiled = event_type |> MappingDefaults.for_type() |> Mapper.compile!()
    config_id = MappingDefaults.config_id(event_type)
    true = :ets.insert(@table, {event_type, compiled, config_id})
    {compiled, config_id}
  end
end
