defmodule Logflare.Sources.Source.RateCounterServer do
  @moduledoc """
  Establishes requests per second per source table. Watches the counters for source tables and periodically pulls them to establish
  events per second. Also handles storing those in the database.
  """
  use GenServer

  alias Logflare.Backends
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.PubSubRates
  alias Logflare.SingleTenant
  alias Logflare.Source.Data
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Sources.Source.Data
  alias Logflare.Sources.Source.RateCounterServer

  require Logger

  @default_bucket_width 60
  @ets_table_name :rate_counters

  use TypedStruct

  typedstruct do
    field :source_id, atom(), enforce: false
    field :count, non_neg_integer(), default: 0
    field :last_rate, non_neg_integer(), default: 0
    field :begin_time, non_neg_integer(), enforce: false
    field :max_rate, non_neg_integer(), default: 0

    field :buckets, map,
      default: %{
        @default_bucket_width => %{
          queue:
            List.duplicate(0, @default_bucket_width) |> LQueue.from_list(@default_bucket_width),
          average: 0,
          sum: 0,
          duration: @default_bucket_width
        }
      }
  end

  @rate_period 1_000

  def start_link(args) do
    source = Keyword.get(args, :source)

    GenServer.start_link(
      __MODULE__,
      args,
      name: Backends.via_source(source, __MODULE__)
    )
  end

  def init(args) do
    source = Keyword.get(args, :source)

    setup_ets_table(source.token)
    {:ok, source.token, {:continue, :boot}}
  end

  def handle_continue(:boot, source_token) do
    put_current_rate()

    bigquery_project_id =
      if !SingleTenant.postgres_backend?() do
        GenUtils.get_project_id(source_token)
      end

    init_counters(source_token, bigquery_project_id)

    RateCounterServer.get_data_from_ets(source_token)
    |> RateCounterServer.broadcast()

    {:noreply, source_token}
  end

  def handle_info(:put_rate, source_id) when is_atom(source_id) do
    {:ok, new_count} = get_insert_count(source_id)
    state = RateCounterServer.get_data_from_ets(source_id)
    %RateCounterServer{} = state = update_state(state, new_count)

    update_ets_table(state)

    if should_broadcast?(source_id) do
      RateCounterServer.broadcast(state)
    end

    put_current_rate()
    {:noreply, source_id}
  end

  @spec new(atom) :: __MODULE__.t()
  def new(source_id) when is_atom(source_id) do
    %RateCounterServer{begin_time: System.monotonic_time(), source_id: source_id}
  end

  @spec update_state(RateCounterServer.t(), non_neg_integer) :: RateCounterServer.t()
  def update_state(%RateCounterServer{} = state, new_count) do
    state
    |> update_current_rate(new_count)
    |> update_max_rate()
    |> update_buckets()
  end

  def update_ets_table(%RateCounterServer{} = state) do
    insert_to_ets_table(state.source_id, state)
  end

  def state_to_external(%RateCounterServer{} = state) do
    %{
      last_rate: lr,
      max_rate: mr,
      buckets: %{
        @default_bucket_width => bucket
      }
    } = state

    limiter_metrics =
      bucket
      |> Map.drop([:queue])

    %{last_rate: lr, average_rate: bucket.average, max_rate: mr, limiter_metrics: limiter_metrics}
  end

  def update_max_rate(%RateCounterServer{max_rate: mx, last_rate: lr} = s) do
    %{s | max_rate: Enum.max([mx, lr])}
  end

  def update_current_rate(%RateCounterServer{} = state, new_count) do
    %{state | last_rate: new_count - state.count, count: new_count}
  end

  def update_buckets(%RateCounterServer{} = state) do
    Map.update!(state, :buckets, fn buckets ->
      for {length, bucket} <- buckets, into: Map.new() do
        # TODO: optimize by not recalculating total sum and average
        new_queue = LQueue.push(bucket.queue, state.last_rate)

        stats =
          new_queue
          |> Enum.to_list()
          |> stats()

        {length, %{bucket | queue: new_queue, average: stats.avg, sum: stats.sum}}
      end
    end)
  end

  @doc """
  Gets last rate
  """
  @spec get_rate(atom) :: integer
  def get_rate(source_id) when is_atom(source_id) do
    source_id
    |> RateCounterServer.get_data_from_ets()
    |> Map.get(:last_rate)
  end

  @doc """
  Gets average rate for the default bucket
  """
  @spec get_avg_rate(atom) :: integer
  def get_avg_rate(source_id) when is_atom(source_id) do
    source_id
    |> RateCounterServer.get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.get(:average)
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source_id) when is_atom(source_id) do
    source_id
    |> RateCounterServer.get_data_from_ets()
    |> Map.get(:max_rate)
  end

  def should_broadcast?(source_id) when is_atom(source_id) do
    source_id
    |> RateCounterServer.get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.get(:queue)
    |> Enum.any?(fn x -> x > 0 end)
  end

  @spec get_rate_metrics(atom, atom) :: map
  def get_rate_metrics(source_id, bucket \\ :default)
      when bucket == :default and is_atom(source_id) do
    source_id
    |> RateCounterServer.get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.drop([:queue])
  end

  defp setup_ets_table(source_id) when is_atom(source_id) do
    initial = RateCounterServer.new(source_id)

    insert_to_ets_table(source_id, initial)
  end

  @spec get_data_from_ets(atom) :: map
  def get_data_from_ets(source_id) do
    if ets_table_is_undefined?(source_id) do
      Logger.error("RateCounterServer: ETS table #{name(source_id)} is undefined")
      data = [{source_id, RateCounterServer.new(source_id)}]
      data[source_id]
    else
      data = :ets.lookup(@ets_table_name, source_id)

      if data[source_id] do
        data[source_id]
      else
        data = [{source_id, RateCounterServer.new(source_id)}]
        data[source_id]
      end
    end
  end

  def ets_table_is_undefined?(_source_id) do
    :ets.whereis(@ets_table_name) == :undefined
  end

  def lookup_ets(source_id) do
    :ets.lookup(@ets_table_name, source_id)
  end

  def insert_to_ets_table(source_id, payload) when is_atom(source_id) do
    if ets_table_is_undefined?(source_id) do
      Logger.debug("#{@ets_table_name} should be defined but it isn't")
    else
      :ets.insert(@ets_table_name, {source_id, payload})
    end
  end

  defp put_current_rate(rate_period \\ @rate_period) do
    Process.send_after(self(), :put_rate, rate_period)
  end

  def name(source_id) do
    String.to_atom("#{source_id}" <> "-rate")
  end

  def broadcast(%RateCounterServer{} = state) do
    local_rates = %{Node.self() => state_to_external(state)}

    PubSubRates.Cache.cache_rates(state.source_id, local_rates)
  end

  @spec get_insert_count(atom) :: {:ok, non_neg_integer()}
  def get_insert_count(source_id) when is_atom(source_id) do
    Sources.Counters.get_inserts(source_id)
  end

  @doc """
  Takes a list of integers representing per second counts of events
  of a source and returns the `avg` and `sum` of those counts.

  ## Examples

      iex> Logflare.Sources.Source.RateCounterServer.stats([12,5,30,5,1000])
      %{avg: 211, sum: 1052}
      iex> Logflare.Sources.Source.RateCounterServer.stats([])
      %{avg: 0, sum: 0}

  """

  @spec stats([integer()]) :: %{avg: integer(), sum: integer()}
  def stats(xs) when is_list(xs) do
    {total, count} =
      Enum.reduce(xs, {0, 0}, fn v, {total, count} ->
        {total + v, count + 1}
      end)

    avg =
      case {total, count} do
        {0, 0} -> 0
        _ -> Kernel.ceil(total / count)
      end

    %{
      avg: avg,
      sum: total
    }
  end

  defp init_counters(source_id, bigquery_project_id) when is_atom(source_id) do
    log_count =
      if bigquery_project_id do
        Data.get_log_count(source_id, bigquery_project_id)
      else
        # TODO: pull total count from PG table
        0
      end

    try do
      Counters.delete(source_id)
    rescue
      _e in ArgumentError ->
        :noop
    after
      Counters.create(source_id)
    end

    Counters.increment(source_id, 0)
    Counters.increment_bq_count(source_id, log_count)
  end
end
