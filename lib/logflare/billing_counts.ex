defmodule Logflare.BillingCounts do
  @moduledoc """
  The context for getting counts for metered billing.
  """

  require Logger

  import Ecto.Query, warn: false
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.BillingCounts.BillingCount

  def timeseries(%User{id: user_id}, start_date, end_date) do
    from(c in BillingCount,
      right_join:
        range in fragment(
          "select generate_series(date(?), date(?), '1 day') AS day, cast(? as bigint) as user_id",
          ^start_date,
          ^end_date,
          ^user_id
        ),
      on: range.day == fragment("date(?)", c.inserted_at),
      on: range.user_id == c.user_id,
      where: range.day >= ^start_date and range.day <= ^end_date,
      group_by: range.day,
      order_by: [asc: range.day],
      select: [
        range.day,
        coalesce(sum(c.count), 0),
        "Log Events"
      ]
    )
    |> Repo.all()
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
end
