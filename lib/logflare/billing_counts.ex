defmodule Logflare.BillingCounts do
  @moduledoc """
  The context for getting counts for metered billing.
  """

  require Logger

  import Ecto.Query, warn: false
  use Logflare.Commons
  alias Logflare.Queries.AnalyticalQueries

  def timeseries(%User{id: user_id}, start_date, end_date) do
    q =
      from(c in BillingCount,
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
      AnalyticalQueries.wrap_in_generated_timeseries_subquery(
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
    |> RepoWithCache.all()
  end

  def insert(user, source, params) do
    assoc = params |> assoc(user) |> assoc(source)

    RepoWithCache.insert(assoc)
  end

  defp assoc(params, user_or_source) do
    Ecto.build_assoc(user_or_source, :billing_counts, params)
  end
end
