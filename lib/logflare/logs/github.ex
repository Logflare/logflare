defmodule Logflare.Logs.Github do
  @moduledoc """
  Formats log events from Github callbacks.
  """
  require Logger

  def handle_batch(batch, _source) when is_list(batch) do
    for msg <- batch do
      %{"message" => custom_message(msg), "metadata" => drop_url_keys(msg)}
    end
  end

  defp drop_url_keys(msg) when is_map(msg) do
    Iteraptor.filter(msg, fn {k, _} -> !has_url_key?(k) end, yield: :none)
  end

  defp has_url_key?(keys) when is_list(keys) do
    Enum.any?(keys, &ends_with/1)
  end

  defp ends_with(key) when is_binary(key) do
    String.ends_with?(key, "_url")
  end

  defp ends_with(key), do: key

  defp custom_message(event) do
    "#{event["repository"]["full_name"]} | #{event["sender"]["login"]} | #{event["action"]}"
  end
end
