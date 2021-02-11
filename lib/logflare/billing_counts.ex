defmodule Logflare.BillingCounts do
  @moduledoc """
  The context for getting counts for metered billing.
  """

  require Logger

  import Ecto.Query, warn: false
  use Logflare.Commons

  def latest_by(node: name, source_id: source_id) when is_atom(name) do
    latest_by(node: Atom.to_string(name), source_id: source_id)
  end

  def latest_by(node: name) when is_atom(name) do
    latest_by(node: Atom.to_string(name))
  end

  def latest_by(kv) do
    BillingCount
    |> where(^kv)
    |> order_by(desc: :inserted_at)
    |> limit(1)
    |> RepoWithCache.one()
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
