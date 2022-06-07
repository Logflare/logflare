defmodule Logflare.Billing.BillingCounts do
  @moduledoc """
  The sub-context for getting counts for metered billing.
  """
  require Logger
  import Ecto.Query, warn: false
  alias Logflare.{User, Repo}
  alias __MODULE__
  def timeseries(%User{id: user_id}, start_date, end_date) do
    q =
      from(c in Count,
        where: c.user_id == ^user_id,
        where: c.inserted_at >= ^start_date and c.inserted_at <= ^end_date,
        group_by: fragment("date(?)", c.inserted_at),
        order_by: [asc: fragment("date(?)", c.inserted_at)],
        select: %{
          date: fragment("date(?)", c.inserted_at),
          sum: sum(c.count)
        }
      )

    q_wrapped =
      wrap_in_generated_timeseries_subquery(
        q,
        :date,
        [:sum],
        start_date,
        end_date
      )

    q_select =
      from(subquery(q_wrapped))
      |> order_by([t], asc: t.date)
      |> select([t], [t.date, t.sum, "Log Events"])

    Repo.all(q_select)
  end

  def timeseries_to_ext(timeseries) do
    Enum.map(timeseries, fn [x, y, z] -> [Calendar.strftime(x, "%b %d"), y, z] end)
  end

  def list_by(kv) do
    BillingCount
    |> where(^kv)
    |> Repo.all()
  end

  def insert(user, source, params) do
    assoc = params |> assoc(user) |> assoc(source)
    Repo.insert(assoc)
  end

  defp assoc(params, user_or_source) do
    Ecto.build_assoc(user_or_source, :billing_counts, params)
  end

  @doc """
  Generates and wraps a given query in a timeseries query, for analysis.
  Currently only used for metered billing
  e.g. https://elixirforum.com/t/help-understanding-difference-between-2-queries/41743/4
  """
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
