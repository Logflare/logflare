defmodule Logflare.Queries.AnalyticalQueries do
  import Ecto.Query

  def wrap_in_generated_timeseries_subquery(
        query,
        group_by_column = :date,
        aggregated_columns,
        start_datetime,
        end_datetime
      )
      when is_list(aggregated_columns) do
    from(subquery(query), as: :t)
    |> join(
      :full,
      [t: t],
      t_nulls in fragment(
        "SELECT generate_series(DATE(?), DATE(?), '1 day') as date",
        ^start_datetime,
        ^end_datetime
      ),
      on: field(t_nulls, ^group_by_column) == field(t, ^group_by_column),
      as: :t_nulls
    )
    |> select([t: t, t_nulls: t_nulls], %{
      date: coalesce(field(t, ^group_by_column), field(t_nulls, ^group_by_column))
    })
    |> select_merge([t: t], %{sum: coalesce(field(t, :sum), 0)})

    # for c <- aggregated_columns, reduce: q do
    #   q -> select_merge(q, [t: t], %{^c => coalesce(field(t, ^c), 0)})
    # end
  end
end
