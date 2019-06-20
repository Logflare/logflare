defmodule Logflare.Source do
  use Ecto.Schema
  import Ecto.Changeset
  @default_source_api_quota 50

  defmodule Metrics do
    use Ecto.Schema

    embedded_schema do
      field :rate, :integer
      field :latest, :integer
      field :avg, :integer
      field :max, :integer
      field :buffer, :integer
      field :inserts, :integer
      field :rejected, :integer
    end
  end

  schema "sources" do
    field :name, :string
    field :token, Ecto.UUID.Atom
    field :public_token, :string
    field :favorite, :boolean, default: false
    field :user_email_notifications, :boolean, default: false
    field :other_email_notifications, :string
    field :user_text_notifications, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota

    belongs_to :user, Logflare.User
    has_many :rules, Logflare.Rule
    field :metrics, :map, virtual: true
    field :has_rejected_events?, :boolean, default: false, virtual: true

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :favorite,
      :user_email_notifications,
      :other_email_notifications,
      :user_text_notifications,
      :bigquery_table_ttl,
      :api_quota
    ])
    |> default_validations()
  end

  def update_by_user_changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :favorite,
      :user_email_notifications,
      :other_email_notifications,
      :user_text_notifications,
      :bigquery_table_ttl
    ])
    |> default_validations()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
  end
end
