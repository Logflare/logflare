defmodule Logflare.Logs.Search.Query do
  import Ecto.Query

  def limit_aggregate_chart_period(query, search_chart_period)
      when search_chart_period in ~w(day hour minute second)a do
    case search_chart_period do
      :day -> limit(query, 30)
      :hour -> limit(query, 168)
      :minute -> limit(query, 120)
      :second -> limit(query, 180)
    end
  end
end
