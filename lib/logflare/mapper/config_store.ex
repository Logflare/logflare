defmodule Logflare.Mapper.ConfigStore do
  @moduledoc """
  Global singleton GenServer that compiles and caches the default OTEL mapping
  configs, one per `{event_type, output_format}`.

  NDJSON options are passed as `{:ndjson, opts}` (see `OutputFormat.ndjson/2`).
  They are normalized before caching, so equivalent options share one entry and
  options equal to the defaults share the `:ndjson` entry.

  Compiled NIF references are stored in a dedicated ETS table for fast,
  concurrent reads from pipeline processes. ClickHouse RowBinary configs are
  compiled eagerly in `init/1`; other formats compile on first request. If the
  GenServer restarts, `init/1` recreates the table and recompiles.

  Started globally from `Logflare.Backends.Supervisor`.
  """

  use GenServer

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  require Logger

  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig.OutputFormat
  alias Logflare.Mapper.OtelDefaults

  @table :mapper_config_store
  @event_types [:log, :metric, :trace]
  @output_formats [:ch_row_binary, :ndjson, :map]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_arg) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Returns the compiled mapping reference and config ID for the given event
  type and output format.

  On a cache miss (a format not compiled eagerly, or a crash/restart race),
  compiles in the calling process and caches the result.
  """
  @spec get_compiled(TypeDetection.event_type(), OtelDefaults.output_format()) ::
          {:ok, reference(), String.t()}
  def get_compiled(event_type, output_format)
      when is_event_type(event_type) and output_format in @output_formats do
    fetch({event_type, output_format})
  end

  def get_compiled(event_type, {:ndjson, opts})
      when is_event_type(event_type) and is_list(opts) do
    fetch({event_type, ndjson_key(OutputFormat.normalize_ndjson_opts(opts))})
  end

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])

    Enum.each(@event_types, &compile_and_store({&1, :ch_row_binary}))

    Logger.info("Mapping configs compiled and cached", event_types: @event_types)
    {:ok, %{}}
  end

  @spec fetch({TypeDetection.event_type(), OtelDefaults.output_format()}) ::
          {:ok, reference(), String.t()}
  defp fetch(key) do
    case :ets.lookup(@table, key) do
      [{^key, compiled, config_id}] ->
        {:ok, compiled, config_id}

      [] ->
        {compiled, config_id} = compile_and_store(key)
        {:ok, compiled, config_id}
    end
  end

  @spec ndjson_key(OutputFormat.ndjson_opts()) :: OtelDefaults.output_format()
  defp ndjson_key(opts) do
    if opts == OutputFormat.ndjson_defaults(), do: :ndjson, else: {:ndjson, opts}
  end

  @spec compile_and_store({TypeDetection.event_type(), OtelDefaults.output_format()}) ::
          {reference(), String.t()}
  defp compile_and_store({event_type, output_format} = key) do
    compiled = event_type |> OtelDefaults.for_type(output_format) |> Mapper.compile!()
    config_id = OtelDefaults.config_id(event_type)
    true = :ets.insert(@table, {key, compiled, config_id})
    {compiled, config_id}
  end
end
