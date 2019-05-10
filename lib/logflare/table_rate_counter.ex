defmodule Logflare.TableRateCounter do
  @moduledoc """
  Establishes requests per second per source table. Watches the counters for source tables and periodically pulls them to establish
  events per second. Also handles storing those in the database.
  """
  use GenServer

  require Logger
  alias __MODULE__, as: TRC

  alias Logflare.TableCounter
  alias Number.Delimit

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
        @default_bucket_width => %{queue: LQueue.new(@default_bucket_width), average_rate: 0}
      }
  end

  @rate_period 1_000
  @ets_table_name :table_rate_counters

  def start_link(source_id, init_count) when is_atom(source_id) and is_integer(init_count) do
    GenServer.start_link(
      __MODULE__,
      source_id,
      name: name(source_id)
    )
  end

  def init(source_id) do
    Logger.info("Rate counter started: #{source_id}")
    setup_ets_table(source_id)
    put_current_rate()

    {:ok, source_id}
  end

  def handle_info(:put_rate, source_id) when is_atom(source_id) do
    put_current_rate()

    {:ok, new_count} = get_new_insert_count(source_id)
    state = get(source_id)

    %TRC{} = state = update_state(state, new_count)


    update_ets_table(state)
    broadcast(state)

    {:noreply, source_id}
  end

  def new(source_id) when is_atom(source_id) do
    %TRC{begin_time: System.monotonic_time(), source_id: source_id}
  end

  def get_new_insert_count(source_id) when is_atom(source_id) do
    table_counter().get_inserts(source_id)
  end

  def update_state(%TRC{} = state, new_count) do
    state
    |> update_current_rate(new_count)
    |> update_max_rate()
    |> update_buckets()
  end

  def update_ets_table(%TRC{} = state) do
    insert_to_ets_table(state.source_id, state)
  end

  def state_to_external(%TRC{} = state) do
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

  def update_max_rate(%TRC{max_rate: mx, last_rate: lr} = s) do
    %{s | max_rate: Enum.max([mx, lr])}
  end

  def update_current_rate(%TRC{} = state, new_count) do
    %{state | last_rate: new_count - state.count, count: new_count}
  end

  def update_buckets(%TRC{} = state) do
    Map.update!(state, :buckets, fn buckets ->
      for {length, bucket} <- buckets, into: Map.new() do
        new_queue = LQueue.push(bucket.queue, state.last_rate)

        average =
          new_queue
          |> Enum.to_list()
          |> average()
          |> round()

        {length,
         %{
           queue: new_queue,
           average: average
         }}
      end
    end)
  end

  @spec get_rate(atom) :: integer
  @doc """
  Gets last rate
  """
  def get_rate(source_id) do
    source_id
    |> get()
    |> Map.get(:last_rate)
  end

  @spec get_avg_rate(atom) :: integer
  @doc """
  Gets average rate for the default bucket
  """
  def get_avg_rate(source_id) when is_atom(source_id) do
    source_id
    |> get()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.get(:average)
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source_id) when is_atom(source_id) do
    source_id
    |> get()
    |> Map.get(:max_rate)
  end

  defp setup_ets_table(source_id) when is_atom(source_id) do
    initial = TRC.new(source_id)

    if ets_table_is_undefined?() do
      table_args = [:named_table, :public]
      :ets.new(@ets_table_name, table_args)
    end

    insert_to_ets_table(source_id, initial)
  end

  def ets_table_is_undefined?() do
    :ets.info(@ets_table_name) == :undefined
  end

  def lookup_ets(source_id) do
    :ets.lookup(@ets_table_name, source_id)
  end

  def insert_to_ets_table(source_id, payload) when is_atom(source_id) do
    if ets_table_is_undefined? do
      Logger.debug("#{@ets_table_name} should be defined but it isn't" )
    end
    :ets.insert(@ets_table_name, {source_id, payload})
  end

  def get(source_id) do
    if ets_table_is_undefined?() do
      0
    else
      data = :ets.lookup(@ets_table_name, source_id)
      data[source_id]
    end
  end

  defp put_current_rate(rate_period \\ @rate_period) do
    Process.send_after(self(), :put_rate, rate_period)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-rate")
  end

  defp broadcast(%TRC{} = state) do
    payload = state_to_external(state)
    source_string = Atom.to_string(state.source_id)

    payload = %{
      source_token: source_string,
      rate: Delimit.number_to_delimited(payload.last_rate),
      average_rate: Delimit.number_to_delimited(payload.average_rate),
      max_rate: Delimit.number_to_delimited(payload.max_rate)
    }

    case :ets.info(LogflareWeb.Endpoint) do
      :undefined ->
        Logger.error("Endpoint not up yet!")

      _ ->
        LogflareWeb.Endpoint.broadcast(
          "dashboard:" <> source_string,
          "dashboard:#{source_string}:rate",
          payload
        )
    end
  end

  def table_counter do
    if Mix.env() == :test do
      Logflare.TableCounterMock
    else
      Logflare.TableCounter
    end
  end

  def average(xs) when is_list(xs) do
    Enum.sum(xs) / length(xs)
  end
end
