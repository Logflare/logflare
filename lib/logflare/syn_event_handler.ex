defmodule Logflare.SynEventHandler do
  @moduledoc """
  Event handler for syn

  Always keep oldest proces.
  """

  @behaviour :syn_event_handler

  require Logger
  @impl true
  def resolve_registry_conflict(scope, name, pid1, pid2) do
    Logger.warning(
      "Resolving registry conflict for #{scope}, #{inspect(name)}, #{inspect(pid1)} and #{inspect(pid2)}. Keeping #{inspect(pid1)}"
    )

    pid1
  end
end
