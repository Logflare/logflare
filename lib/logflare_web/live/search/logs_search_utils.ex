defmodule Logflare.Logs.Search.Utils do
  @moduledoc """
  Utilities for Logs search and Logs live view modules
  """
  require Logger

  def format_error(%Tesla.Env{body: body}) do
    body
    |> Poison.decode!()
    |> Map.get("error")
    |> Map.get("message")
  end

  def format_error(e), do: e

  def gen_search_tip() do
    tips = [
      "Search is case sensitive.",
      "Exact match an integer (e.g. `metadata.response.status:500`).",
      "Integers support greater and less than symobols (e.g. `metadata.response.origin_time:<1000`).",
      ~s|Exact match a string in a field (e.g. `metadata.response.cf-ray:"505c16f9a752cec8-IAD"`).|,
      "Timestamps support greater and less than symbols (e.g. `timestamp:>=2019-07-01`).",
      ~s|Match a field with regex (e.g. `metadata.browser:~"Firefox 5\\d"`).|,
      "Search between times with multiple fields (e.g. `timestamp:>=2019-07-01 timestamp:<=2019-07-02`).",
      "Default behavoir is to search the log message field (e.g. `error`).",
      "Turn off Live Search to search the full history of this source."
    ]

    Enum.random(tips)
  end

  def put_result_in(_, so, path \\ nil)
  def put_result_in(:ok, so, _), do: so
  def put_result_in({:ok, value}, so, path) when is_atom(path), do: %{so | path => value}
  def put_result_in({:error, term}, so, _), do: %{so | error: term}

  def maybe_string_to_integer(nil), do: 0
  def maybe_string_to_integer(s) when is_binary(s), do: String.to_integer(s)
end
