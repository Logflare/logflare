defmodule Logflare.SystemMetrics.AllLogsLogged do
  @moduledoc false
  use GenServer

  alias Logflare.Repo
  alias Logflare.SystemMetric

  require Logger

  import Ecto.Query, only: [from: 2]

  @total_logs :total_logs_logged
  @table :system_counter
  @persist_every 60_000

  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  def init(state) do
    persist()

    query = from m in SystemMetric, select: m, limit: 1, order_by: [desc: m.inserted_at]

    case total_logs = Repo.one(query) do
      nil ->
        create(@total_logs, 0)

      _ ->
        create(@total_logs, total_logs.all_logs_logged)
    end

    {:ok, state}
  end

  def handle_info(:persist, state) do
    persist()

    {:ok, log_count} = log_count(@total_logs)

    Repo.insert(%SystemMetric{all_logs_logged: log_count})

    {:noreply, state}
  end

  ## Public Functions

  @spec create(atom, integer()) :: {:ok, atom}
  def create(metric, count \\ 0) do
    :ets.new(@table, [:public, :named_table])

    :ets.update_counter(@table, metric, {2, 0}, {metric, 0, 0})
    :ets.update_counter(@table, metric, {3, count}, {metric, 0, 0})

    {:ok, metric}
  end

  @spec incriment(atom) :: {:ok, atom}
  def incriment(metric) do
    :ets.update_counter(@table, metric, {2, 1}, {metric, 0, 0})

    {:ok, metric}
  end

  @spec incriment(atom, integer) :: {:ok, atom}
  def incriment(metric, count) do
    :ets.update_counter(@table, metric, {2, count}, {metric, 0, 0})

    {:ok, metric}
  end

  @spec log_count(atom) :: {:ok, non_neg_integer}
  def log_count(metric) do
    [{_metric, inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)
    count = inserts_since_init + init_log_count

    {:ok, count}
  end

  def init_log_count(metric) do
    [{_metric, _inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)

    {:ok, init_log_count}
  end

  def all_metrics(metric) do
    [{_metric, inserts_since_init, init_log_count}] = :ets.lookup(@table, metric)
    total = inserts_since_init + init_log_count

    {:ok, %{inserts_since_init: inserts_since_init, init_log_count: init_log_count, total: total}}
  end

  ## Private Functions

  defp persist() do
    Process.send_after(self(), :persist, @persist_every)
  end
end
