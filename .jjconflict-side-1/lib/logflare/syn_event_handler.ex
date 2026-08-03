defmodule Logflare.SynEventHandler do
  @moduledoc """
  Event handler for syn

  Always keep oldest proces.
  """
  @behaviour :syn_event_handler

  require Logger
  @impl true

  @doc """
  Resolves registry conflicts for scopes.

  ## Examples

      iex> pid1 = :c.pid(0,111,0)
      iex> pid2 = :c.pid(0,222,0)
      iex> meta1 = %{timestamp: 1, sup_pid: pid1}
      iex> meta2 = %{timestamp: 2, sup_pid: pid2}
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:core, Logflare.SomeModule, {pid1, meta1, 1}, {pid2, meta2, 2})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:core, Logflare.SomeModule, {pid2, meta2, 2}, {pid1, meta1, 1})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:core, Logflare.SomeModule, {pid2, meta2, 1}, {pid1, meta1, 1})
      #PID<0.111.0>
      iex> Logflare.SynEventHandler.resolve_registry_conflict(:other, Logflare.OtherModule, {pid2, %{}, 2}, {pid1, %{}, 1})
      #PID<0.111.0>

  """
  def resolve_registry_conflict(scope, name, pid_meta1, pid_meta2) do
    {original, to_stop} = keep_original(pid_meta1, pid_meta2)

    if node() == node(to_stop) do
      {pid1, meta1, _} = pid_meta1
      {pid2, meta2, _} = pid_meta2
      local_indicator = if node() == node(original), do: "local", else: "remote"

      Logger.debug(
        "#{__MODULE__}  [#{node()}|registry<#{scope}>] Registry CONFLICT for name #{inspect(name)}: {#{inspect(pid1)}, #{inspect(meta1)}} vs {#{inspect(pid2)}, #{inspect(meta2)}} -> keeping #{local_indicator}: #{inspect(original)}"
      )

      Logflare.Utils.try_to_stop_process(to_stop, :shutdown, :syn_resolve_kill)
    end

    original
  end

  defp keep_original(
         {pid1, %{timestamp: timestamp1}, _timestamp1},
         {pid2, %{timestamp: timestamp2}, _timestamp2}
       ) do
    if timestamp1 < timestamp2 do
      {pid1, pid2}
    else
      {pid2, pid1}
    end
  end

  # fallback if the :timestamp metadata with higher nanosecond resolution is not,
  defp keep_original(
         {pid1, _meta1, timestamp1},
         {pid2, _meta2, timestamp2}
       ) do
    if timestamp1 < timestamp2 do
      {pid1, pid2}
    else
      {pid2, pid1}
    end
  end
end
