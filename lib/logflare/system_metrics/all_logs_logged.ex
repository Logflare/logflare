defmodule Logflare.SystemMetrics.AllLogsLogged do
  use GenServer

  require Logger

  @total_logs :total_logs_logged
  @og_total_logs 112_935
  @table :system_counter
  @table_path "system-table"
  @persist_every 60_000

  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(state) do
    tab_path = @table_path <> "/" <> Atom.to_string(@table) <> ".tab"

    case :ets.tabfile_info(String.to_charlist(tab_path)) do
      {:ok, _info} ->
        Logger.info("System stats loaded!")
        :ets.file2tab(String.to_charlist(tab_path))

      {:error, _reason} ->
        Logger.info("System stats started!")
        :ets.new(@table, [:public, :named_table])
        create(@total_logs)
        incriment(@total_logs, @og_total_logs)
    end

    persist()

    {:ok, state}
  end

  def handle_info(:persist, state) do
    case File.stat(@table_path) do
      {:ok, _stats} ->
        persist_tables(@table)

      {:error, _reason} ->
        File.mkdir(@table_path)
        persist_tables(@table)
    end

    persist()
    {:noreply, state}
  end

  ## Public Functions

  @spec create(atom) :: {:ok, atom}
  def create(metric) do
    :ets.update_counter(@table, metric, {2, 0}, {metric, 0, 0})
    :ets.update_counter(@table, metric, {3, 0}, {metric, 0, 0})
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
    [{_metric, inserts, _deletes}] = :ets.lookup(@table, metric)
    count = inserts
    {:ok, count}
  end

  ## Private Functions

  defp persist() do
    Process.send_after(self(), :persist, @persist_every)
  end

  defp persist_tables(table) do
    tab_path = @table_path <> "/" <> Atom.to_string(table) <> ".tab"
    :ets.tab2file(table, String.to_charlist(tab_path))
  end
end
