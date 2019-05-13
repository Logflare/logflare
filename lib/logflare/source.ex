defmodule Logflare.Source do
  use Ecto.Schema
  import Ecto.Changeset
  @default_source_api_quota 5

  schema "sources" do
    field :name, :string
    field :token, Ecto.UUID.Atom
    field :public_token, :string
    field :overflow_source, Ecto.UUID
    field :avg_rate, :integer, virtual: true
    field :favorite, :boolean, default: false
    field :user_email_notifications, :boolean, default: false
    field :other_email_notifications, :string
    field :user_text_notifications, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota

    belongs_to :user, Logflare.User
    has_many :rules, Logflare.Rule

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :overflow_source,
      :avg_rate,
      :favorite,
      :user_email_notifications,
      :other_email_notifications,
      :user_text_notifications,
      :bigquery_table_ttl,
      :api_quota
    ])
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
    |> validate_min_avg_source_rate(:avg_rate)
  end

  def validate_min_avg_source_rate(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, avg_rate ->
      case avg_rate >= 1 do
        true ->
          []

        false ->
          [{field, options[:message] || "Average events per second must be at least 1"}]
      end
    end)
  end
end
