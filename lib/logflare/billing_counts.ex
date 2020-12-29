defmodule Logflare.BillingCounts do
  @moduledoc """
  The context for getting counts for metered billing.
  """

  require Logger

  import Ecto.Query, warn: false
  alias Logflare.Repo
  alias Logflare.BillingCounts.BillingCount

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
