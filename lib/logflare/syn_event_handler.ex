defmodule Logflare.SynEventHandler do
  @moduledoc """
  Event handler for syn

  Always keep oldest proces.
  """

  @behaviour :syn_event_handler

  require Logger
  @impl true
  def resolve_registry_conflict(
        :alerting,
        Logflare.Alerting.AlertsScheduler,
        pid_meta1,
        pid_meta2
      ) do
    original = keep_original(pid_meta1, pid_meta2)

    Logger.warning(
      "Resolving registry conflict for alerting, for Logflare.Alerting.AlertsScheduler. Keeping original #{original} on #{node(original)}"
    )

    original
  end

  def resolve_registry_conflict(scope, name, pid_meta1, pid_meta2) do
    Logger.debug(
      "Resolving registry conflict for #{scope}, #{inspect(name)}, #{inspect(pid_meta1)} and #{inspect(pid_meta2)}. Keeping #{inspect(pid_meta1)}"
    )

    keep_original(pid_meta1, pid_meta2)
  end

  defp keep_original(
         {pid1, %{timestamp: timestamp1}, _timestamp1},
         {pid2, %{timestamp: timestamp2}, _timestamp2}
       ) do
    if timestamp1 < timestamp2 do
      pid1
    else
      pid2
    end
    |> dbg()
  end

  # fallback if the :timestamp metadata with higher nanosecond resolution is not,
  defp keep_original({pid1, _meta1, timestamp1}, {pid2, _meta2, timestamp2}) do
    if timestamp1 < timestamp2 do
      pid1
    else
      pid2
    end
  end
end
