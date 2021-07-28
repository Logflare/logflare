defmodule LogflareWeb.SearchLV.Utils do
  @moduledoc """
  Various utility functions for logs search liveviews
  """
  require Logger
  use Phoenix.HTML

  def maybe_cancel_tailing_timer(socket) do
    if socket.assigns.tailing_timer, do: Process.cancel_timer(socket.assigns.tailing_timer)
  end

  def log_lv_received_info(msg, source) do
    # Logger.info("#{pid_sid(source)} received #{msg} info msg...")
  end

  def log_lv(source, msg) do
    # Logger.info("#{pid_sid(source)} #{msg}")
  end

  def log_lv_executing_query(source) do
    # Logger.info("#{pid_sid(source)} executing tail search query...")
  end

  def log_lv_received_event(event_name, source) do
    # Logger.info("#{pid_sid(source)} received #{event_name} event")
  end

  defp pid_sid(source) do
    pid_source_to_string(self(), source)
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
