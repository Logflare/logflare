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
    field :count, non_neg_integer(), enforce: true
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

  def start_link(source, init_count) do
    started_at = System.monotonic_time(:second)

    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        source_id: source,
        count: init_count,
        begin_time: started_at
      },
      name: name(source)
    )
  end

  def init(state) do
    Logger.info("Rate counter started: #{state.source_id}")
    setup_ets_table(state)
    put_current_rate()

    {:ok, state}
  end

  def handle_info(:put_rate, state) do
    put_current_rate()

    {:ok, new_count} = get_new_insert_count(state)

    state = update_state(state, new_count)

    update_ets_table(state)
    broadcast(state)

    {:noreply, state}
  end

  def get_new_insert_count(state) do
    table_counter().get_inserts(state.source_id)
  end

  def update_state(state, new_count) do
    state
    |> update_current_rate(new_count)
    |> update_max_rate()
    |> update_buckets()
  end

  def update_ets_table(state) do
    insert_to_ets_table(state.source_id, state)
  end

  def state_to_external(state) do
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

  def update_max_rate(%{max_rate: mx, last_rate: lr} = s) do
    %{s | max_rate: Enum.max([mx, lr])}
  end

  def update_current_rate(state, new_count) do
    %{state | last_rate: new_count - state.count, count: new_count}
  end

  def update_buckets(%__MODULE__{} = state) do
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
  def get_rate(source) do
    source
    |> get()
    |> Map.get(:last_rate)
  end

  @spec get_avg_rate(atom) :: integer
  @doc """
  Gets average rate for the default bucket
  """
  def get_avg_rate(source) do
    source
    |> get()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.get(:average)
  end

  @spec get_max_rate(atom) :: integer
  def get_max_rate(source) do
    source
    |> get()
    |> Map.get(:max_rate)
  end

  defp setup_ets_table(state) do
    payload = %{last_rate: 0, average_rate: 0, max_rate: 0}

    if ets_table_is_undefined?() do
      table_args = [:named_table, :public]
      :ets.new(@ets_table_name, table_args)
    end

    insert_to_ets_table(state.source_id, payload)
  end

  def ets_table_is_undefined?() do
    :ets.info(@ets_table_name) == :undefined
  end

  def insert_to_ets_table(source_id, payload) do
    :ets.insert(@ets_table_name, {source_id, payload})
  end

  def get(source) do
    if ets_table_is_undefined?() do
      0
    else
      data = :ets.lookup(@ets_table_name, source)
      data[source]
    end
  end

  defp put_current_rate(rate_period \\ @rate_period) do
    Process.send_after(self(), :put_rate, rate_period)
  end

  defp name(source) do
    String.to_atom("#{source}" <> "-rate")
  end

  defp broadcast(state) do
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
