defmodule Logflare.Logs.Search.Utils do
  @moduledoc """
  Utilities for Logs search and Logs live view modules
  """
  require Logger

  def pid_to_string(pid) when is_pid(pid) do
    pid
    |> :erlang.pid_to_list()
    |> to_string()
  end

  def pid_source_to_string(pid, source) do
    "#{pid_to_string(pid)} for #{source.token}"
  end

  def format_error(%Tesla.Env{body: body}) do
    body
    |> Poison.decode!()
    |> Map.get("error")
    |> Map.get("message")
  end

  def format_error(e), do: e

  def log_lv_received_info(msg, source) do
    Logger.info("#{pid_sid(source)} received #{msg} info msg...")
  end

  def log_lv(source, msg) do
    Logger.info("#{pid_sid(source)} #{msg}")
  end

  def log_lv_executing_query(source) do
    Logger.info("#{pid_sid(source)} executing tail search query...")
  end

  def log_lv_received_event(event_name, source) do
    Logger.info("#{pid_sid(source)} received #{event_name} event")
  end

  defp pid_sid(source) do
    pid_source_to_string(self(), source)
  end
end
