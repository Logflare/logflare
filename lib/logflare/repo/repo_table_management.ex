defmodule Logflare.Repo.TableManagement do
  alias Logflare.Repo
  import Ecto.Query

  def delete_all_rows_over_limit_with_opts(schema, opts) do
    q =
      schema
      |> from()
      |> windows(row_stats: [partition_by: ^opts.partition_by, order_by: ^opts.order_by])
      |> select([t], %{
        id: t.id,
        row_number: over(row_number(), :row_stats)
      })

    schema
    |> from()
    |> join(:inner, [t], row_data in subquery(q), on: t.id == row_data.id)
    |> where([t, row_data], row_data.row_number >= ^opts.limit)
    |> Repo.delete_all()

    :ok
  end
end
