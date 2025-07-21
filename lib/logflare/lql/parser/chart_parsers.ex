defmodule Logflare.Lql.Parser.ChartParsers do
  @moduledoc """
  Chart and aggregation parsing combinators
  """

  import NimbleParsec

  alias Logflare.Lql.Parser.BasicCombinators

  def chart_clause() do
    ignore(choice([string("chart"), string("c")]))
    |> ignore(ascii_char([?:]))
    |> choice([
      chart_aggregate_group_by(),
      chart_aggregate()
    ])
    |> tag(:chart)
  end

  def chart_aggregate() do
    choice([
      string("avg") |> replace(:avg),
      string("count") |> replace(:count),
      string("sum") |> replace(:sum),
      string("max") |> replace(:max),
      string("p50") |> replace(:p50),
      string("p95") |> replace(:p95),
      string("p99") |> replace(:p99)
    ])
    |> unwrap_and_tag(:aggregate)
    |> ignore(string("("))
    |> concat(
      choice([string("*") |> replace("timestamp") |> unwrap_and_tag(:path), metadata_field()])
    )
    |> ignore(string(")"))
  end

  def chart_aggregate_group_by() do
    ignore(string("group_by"))
    |> ignore(string("("))
    |> ignore(choice([string("timestamp"), string("t")]))
    |> ignore(string("::"))
    |> choice([
      string("second") |> replace(:second),
      string("s") |> replace(:second),
      string("minute") |> replace(:minute),
      string("m") |> replace(:minute),
      string("hour") |> replace(:hour),
      string("h") |> replace(:hour),
      string("day") |> replace(:day),
      string("d") |> replace(:day)
    ])
    |> unwrap_and_tag(:period)
    |> ignore(string(")"))
  end

  defdelegate metadata_field(), to: BasicCombinators
end
