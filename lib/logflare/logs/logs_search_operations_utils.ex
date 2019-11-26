defmodule Logflare.Logs.SearchOperations.Utils do
  @moduledoc false
  import Ecto.Query

  def format_agg_row_keys(rows) do
    rows
    |> Enum.map(fn row ->
      row
      |> Enum.map(&agg_row_key_to_names/1)
      |> Map.new()
    end)
  end

  def format_agg_row_values(rows) do
    rows
    |> Enum.map(fn row ->
      row
      |> Enum.map(&agg_row_key_formatter/1)
      |> Map.new()
    end)
  end

  defp agg_row_key_to_names({"f0_", v}), do: {"timestamp", v}
  defp agg_row_key_to_names({"f1_", v}), do: {"value", v}

  defp agg_row_key_formatter({"timestamp", v}) do
    {:ok, v} =
      v
      |> Timex.from_unix(:microseconds)
      |> Timex.format("{RFC822z}")

    {"timestamp", v}
  end

  defp agg_row_key_formatter(x), do: x
end
