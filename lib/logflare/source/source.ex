defmodule Logflare.Source do
  use Ecto.Schema
  alias Logflare.Google.BigQuery.GenUtils
  import Ecto.Changeset
  @default_source_api_quota 50

  defmodule Metrics do
    use Ecto.Schema

    embedded_schema do
      field :rate, :integer
      field :rate_int, :integer
      field :latest, :integer
      field :avg, :integer
      field :avg_int, :integer
      field :max, :integer
      field :max_int, :integer
      field :buffer, :integer
      field :buffer_int, :integer
      field :inserts, :integer
      field :inserts_int, :integer
      field :recent, :integer
      field :recent_int, :integer
      field :rejected, :integer
      field :rejected_int, :integer
      field :schema_fields, :integer
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
    field :bq_table_id, :string, virtual: true

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

  def put_bq_table_id(%__MODULE__{} = source) do
    %{source | bq_table_id: to_bq_table_id(source)}
  end

  def to_bq_table_id(%__MODULE__{} = source) do
    bq_project_id =
      source.user.bigquery_project_id ||
        Application.get_env(:logflare, Logflare.Google)[:project_id]

    env = Application.get_env(:logflare, :env)
    table = GenUtils.format_table_name(source.token)
    dataset = "#{source.user.id}_#{env}"
    "`#{bq_project_id}`.#{dataset}.#{table}"
  end
end
