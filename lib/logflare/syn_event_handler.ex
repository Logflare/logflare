defmodule Logflare.SynEventHandler do
  @moduledoc """
  Event handler for syn

  Always keep oldest proces.
  """
  @behaviour :syn_event_handler

  alias Logflare.GenSingleton

  require Logger
  @impl true

  @doc """
  Resolves registry conflicts for alerting and other scopes.

  ## Examples

      iex> pid1 = :c.pid(0,111,0)
      iex> pid2 = :c.pid(0,222,0)
      iex> meta1 = %{timestamp: 1, sup_pid: pid1}
      iex> meta2 = %{timestamp: 2, sup_pid: pid2}
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:alerting, Logflare.Alerting.AlertsScheduler, {pid1, meta1, 1}, {pid2, meta2, 2})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:alerting, Logflare.Alerting.AlertsScheduler, {pid2, meta2, 2}, {pid1, meta1, 1})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:alerting, Logflare.Alerting.AlertsScheduler, {pid2, meta2, 1}, {pid1, meta1, 1})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:other, Logflare.OtherModule, {pid2, %{}, 2}, {pid1, %{}, 1})
      #PID<0.111.0>

  """

  def resolve_registry_conflict(
        :alerting,
        Logflare.Alerting.AlertsScheduler,
        pid_meta1,
        pid_meta2
      ) do
    {{original, _, _}, {_to_stop, to_stop_meta, _}} = keep_original(pid_meta1, pid_meta2)

    pid_node =
      if is_pid(original) do
        node(original)
      end

    Logger.warning(
      "Resolving registry conflict for alerting, for Logflare.Alerting.AlertsScheduler. Keeping original #{inspect(original)} on #{inspect(pid_node)}"
    )

    # stop the process via GenSingleton
    GenSingleton.stop_local(to_stop_meta.sup_pid)

    original
  end

  def resolve_registry_conflict(scope, name, pid_meta1, pid_meta2) do
    Logger.debug(
      "Resolving registry conflict for #{scope}, #{inspect(name)}, #{inspect(pid_meta1)} and #{inspect(pid_meta2)}. Keeping #{inspect(pid_meta1)}"
    )

    {{original, _, _}, {to_stop, _, _}} = keep_original(pid_meta1, pid_meta2)
    Process.exit(to_stop, :syn_resolve_kill)
    original
  end

  defp keep_original(
         {_pid1, %{timestamp: timestamp1}, _timestamp1} = pid_meta1,
         {_pid2, %{timestamp: timestamp2}, _timestamp2} = pid_meta2
       ) do
    if timestamp1 < timestamp2 do
      {pid_meta1, pid_meta2}
    else
      {pid_meta2, pid_meta1}
    end
  end

  # fallback if the :timestamp metadata with higher nanosecond resolution is not,
  defp keep_original(
         {_pid1, _meta1, timestamp1} = pid_meta1,
         {_pid2, _meta2, timestamp2} = pid_meta2
       ) do
    if timestamp1 < timestamp2 do
      {pid_meta1, pid_meta2}
    else
      {pid_meta2, pid_meta1}
    end
  end
end
