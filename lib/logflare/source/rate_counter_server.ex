defmodule Logflare.Source.RateCounterServer do
  @moduledoc """
  Establishes requests per second per source table. Watches the counters for source tables and periodically pulls them to establish
  events per second. Also handles storing those in the database.
  """
  use GenServer

  require Logger
  alias __MODULE__, as: RCS
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Sources.Counters
  alias Logflare.Source.Data
  alias Logflare.Source

  @default_bucket_width 60

  use TypedStruct
  use Publicist

  typedstruct do
    field :source_id, atom(), enforce: true
    field :count, non_neg_integer(), default: 0
    field :last_rate, non_neg_integer(), default: 0
    field :begin_time, non_neg_integer(), enforce: true
    field :max_rate, non_neg_integer(), default: 0

    field :buckets, map,
      default: %{
        @default_bucket_width => %{
          queue: LQueue.new(@default_bucket_width),
          average: 0,
          sum: 0,
          duration: @default_bucket_width
        }
      }
  end

  @rate_period 1_000
  @ets_table_name :source_rate_counters

  def start_link(%RLS{source_id: source_id}) when is_atom(source_id) do
    GenServer.start_link(
      __MODULE__,
      source_id,
      name: name(source_id)
    )
  end

  def init(source_id) when is_atom(source_id) do
    Process.flag(:trap_exit, true)
    setup_ets_table(source_id)
    put_current_rate()
    bigquery_project_id = GenUtils.get_project_id(source_id)
    init_counters(source_id, bigquery_project_id)

    Logger.info("Rate counter started: #{source_id}")
    {:ok, source_id}
  end

  def handle_info(:put_rate, source_id) when is_atom(source_id) do
    put_current_rate()

    {:ok, new_count} = get_insert_count(source_id)

    state = get_data_from_ets(source_id)

    %RCS{} = state = update_state(state, new_count)

    update_ets_table(state)
    broadcast(state)

    {:noreply, source_id}
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  @spec new(atom) :: __MODULE__.t()
  def new(source_id) when is_atom(source_id) do
    %RCS{begin_time: System.monotonic_time(), source_id: source_id}
  end

  @spec update_state(RCS.t(), non_neg_integer) :: RCS.t()
  def update_state(%RCS{} = state, new_count) do
    state
    |> update_current_rate(new_count)
    |> update_max_rate()
    |> update_buckets()
  end

  def update_ets_table(%RCS{} = state) do
    insert_to_ets_table(state.source_id, state)
  end

  def state_to_external(%RCS{} = state) do
    %{
      last_rate: lr,
      max_rate: mr,
      buckets: %{
        @default_bucket_width => %{
          average: avg
        }
      }
    } = state

    %{last_rate: lr, average_rate: avg, max_rate: mr}
  end

  def update_max_rate(%RCS{max_rate: mx, last_rate: lr} = s) do
    %{s | max_rate: Enum.max([mx, lr])}
  end

  def update_current_rate(%RCS{} = state, new_count) do
    %{state | last_rate: new_count - state.count, count: new_count}
  end

  def update_buckets(%RCS{} = state) do
    Map.update!(state, :buckets, fn buckets ->
      for {length, bucket} <- buckets, into: Map.new() do
        # TODO: optimize by not recalculating total sum and average
        new_queue = LQueue.push(bucket.queue, state.last_rate)

        average =
          new_queue
          |> Enum.to_list()
          |> average()
          |> round()

        sum =
          new_queue
          |> Enum.to_list()
          |> Enum.sum()

        {length, %{bucket | queue: new_queue, average: average, sum: sum}}
      end
    end)
  end

  @doc """
  Gets last rate
  """
  @spec get_rate(atom) :: integer
  def get_rate(source_id) when is_atom(source_id) do
    source_id
    |> get_data_from_ets()
    |> Map.get(:last_rate)
  end

  @doc """
  Gets average rate for the default bucket
  """
  @spec get_avg_rate(atom) :: integer
  def get_avg_rate(source_id) when is_atom(source_id) do
    source_id
    |> get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.get(:average)
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source_id) when is_atom(source_id) do
    source_id
    |> get_data_from_ets()
    |> Map.get(:max_rate)
  end

  @spec get_metrics(atom, atom) :: map
  def get_metrics(source_id, bucket \\ :default) when bucket == :default and is_atom(source_id) do
    source_id
    |> get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.drop([:queue])
  end

  defp setup_ets_table(source_id) when is_atom(source_id) do
    initial = RCS.new(source_id)

    if ets_table_is_undefined?() do
      table_args = [:named_table, :public]
      :ets.new(@ets_table_name, table_args)
    end

    insert_to_ets_table(source_id, initial)
  end

  @spec get_data_from_ets(atom) :: map
  def get_data_from_ets(source_id) do
    if ets_table_is_undefined?() do
      Logger.error("ETS table #{@ets_table_name} is undefined")
      data = [{source_id, RCS.new(source_id)}]
      data[source_id]
    else
      data = :ets.lookup(@ets_table_name, source_id)

      if data[source_id] do
        data[source_id]
      else
        data = [{source_id, RCS.new(source_id)}]
        data[source_id]
      end
    end
  end

  def ets_table_is_undefined?() do
    :ets.info(@ets_table_name) == :undefined
  end

  def lookup_ets(source_id) do
    :ets.lookup(@ets_table_name, source_id)
  end

  def insert_to_ets_table(source_id, payload) when is_atom(source_id) do
    if ets_table_is_undefined?() do
      Logger.debug("#{@ets_table_name} should be defined but it isn't")
    else
      :ets.insert(@ets_table_name, {source_id, payload})
    end
  end

  defp put_current_rate(rate_period \\ @rate_period) do
    Process.send_after(self(), :put_rate, rate_period)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-rate")
  end

  defp broadcast(%RCS{} = state) do
    state
    |> state_to_external()
    |> Map.put(:source_id, state.source_id)
    |> Source.ChannelTopics.broadcast_rates()
  end

  @spec get_insert_count(atom) :: {:ok, non_neg_integer()}
  def get_insert_count(source_id) when is_atom(source_id) do
    Logflare.Sources.Counters.get_inserts(source_id)
  end

  def average(xs) when is_list(xs) do
    Enum.sum(xs) / length(xs)
  end

  defp init_counters(source_id, bigquery_project_id) when is_atom(source_id) do
    log_count = Data.get_log_count(source_id, bigquery_project_id)
    Counters.delete(source_id)
    Counters.create(source_id)
    Counters.incriment_ets_count(source_id, 0)
    Counters.incriment_total_count(source_id, log_count)
  end
end
