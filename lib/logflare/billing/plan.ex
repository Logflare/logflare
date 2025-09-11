defmodule Logflare.Billing.Plan do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  typed_schema "plans" do
    field :name, :string, default: "Legacy"
    field :stripe_id, :string
    field :price, :integer, default: 0
    field :period, :string, default: "month"
    field :limit_sources, :integer, default: 100
    field :limit_rate_limit, :integer, default: 150
    field :limit_source_rate_limit, :integer, default: 50
    field :limit_alert_freq, :integer, default: :timer.minutes(1)
    field :limit_saved_search_limit, :integer, default: 1
    field :limit_team_users_limit, :integer, default: 2
    field :limit_source_fields_limit, :integer, default: 500
    field :limit_source_ttl, :integer, default: :timer.hours(72)
    field :type, :string, default: "standard"

    timestamps()
  end

  @spec changeset(
          {map, map} | %{:__struct__ => atom | %{__changeset__: map}, optional(atom) => any},
          :invalid | %{optional(:__struct__) => none, optional(atom | binary) => any}
        ) :: Ecto.Changeset.t()
  @doc false
  def changeset(plan, attrs) do
    plan
    |> cast(attrs, [
      :name,
      :stripe_id,
      :price,
      :period,
      :limit_sources,
      :limit_rate_limit,
      :limit_source_rate_limit,
      :limit_alert_freq,
      :limit_saved_search_limit,
      :limit_team_users_limit,
      :limit_source_fields_limit,
      :limit_source_ttl,
      :type
    ])
    |> validate_required([
      :name,
      :price,
      :period,
      :limit_sources,
      :limit_rate_limit,
      :limit_source_rate_limit,
      :limit_alert_freq,
      :limit_saved_search_limit,
      :limit_team_users_limit,
      :limit_source_fields_limit,
      :limit_source_ttl,
      :type
    ])
  end
end
