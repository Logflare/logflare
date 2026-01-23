defmodule LogflareWeb.SearchLV.Utils do
  @moduledoc """
  Various utility functions for logs search LiveViews.
  """
  use LogflareWeb, :html

  require Logger

  def maybe_cancel_tailing_timer(socket) do
    if socket.assigns.tailing_timer, do: Process.cancel_timer(socket.assigns.tailing_timer)
  end

  def pid_to_string(pid) when is_pid(pid) do
    pid
    |> :erlang.pid_to_list()
    |> to_string()
  end

  def pid_source_to_string(pid, source) do
    "#{pid_to_string(pid)} for #{source.token}"
  end
end
