defmodule Logflare.AlertIssues.ActiveCache do
  @moduledoc """
  Node-local index of the latest alert issue snapshot per user and fingerprint.

  `Logflare.AlertIssues.apply_signal/2` reads it to dedupe signals and validate
  transitions without querying the backend that stores `system.alert_issues`.
  """
  use GenServer

  @table __MODULE__

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_arg), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @spec get(pos_integer(), String.t()) :: map() | nil
  def get(user_id, fingerprint) do
    case :ets.lookup(@table, {user_id, fingerprint}) do
      [{_key, snapshot}] -> snapshot
      [] -> nil
    end
  end

  @spec put(pos_integer(), String.t(), map()) :: :ok
  def put(user_id, fingerprint, snapshot) do
    :ets.insert(@table, {{user_id, fingerprint}, snapshot})
    :ok
  end

  @spec active_fingerprints(pos_integer(), String.t()) :: [String.t()]
  def active_fingerprints(user_id, alert_token) do
    :ets.select(@table, [
      {{{user_id, :"$1"}, %{"alert_token" => alert_token, "status" => :"$2"}},
       [{:"/=", :"$2", "resolved"}], [:"$1"]}
    ])
  end

  @spec increment_clean_runs(pos_integer(), String.t()) :: pos_integer()
  def increment_clean_runs(user_id, fingerprint) do
    :ets.update_counter(@table, {:clean_runs, user_id, fingerprint}, 1, {
      {:clean_runs, user_id, fingerprint},
      0
    })
  end

  @spec reset_clean_runs(pos_integer(), String.t()) :: :ok
  def reset_clean_runs(user_id, fingerprint) do
    :ets.delete(@table, {:clean_runs, user_id, fingerprint})
    :ok
  end

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, nil}
  end
end
