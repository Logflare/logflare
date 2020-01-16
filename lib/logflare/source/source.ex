defmodule Logflare.Source do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Google.BigQuery.GenUtils
  import Ecto.Changeset
  @default_source_api_quota 50
  @derive {Jason.Encoder, only: [:name, :token, :id]}

  defmodule Metrics do
    @moduledoc false
    use TypedEctoSchema

    typed_embedded_schema do
      field :rate, :integer
      field :latest, :integer
      field :avg, :integer
      field :max, :integer
      field :buffer, :integer
      field :inserts, :integer
      field :inserts_string, :string
      field :recent, :integer
      field :rejected, :integer
      field :fields, :integer
    end
  end

  typed_schema "sources" do
    field :name, :string
    field :token, Ecto.UUID.Atom
    field :public_token, :string
    field :favorite, :boolean, default: false
    field :user_email_notifications, :boolean, default: false
    field :other_email_notifications, :string
    field :user_text_notifications, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota
    field :webhook_notification_url, :string
    field :slack_hook_url, :string

    belongs_to :user, Logflare.User
    has_many :rules, Logflare.Rule
    has_many :saved_searches, Logflare.SavedSearch

    field :metrics, :map, virtual: true
    field :has_rejected_events?, :boolean, default: false, virtual: true
    field :bq_table_id, :string, virtual: true
    field :bq_table_schema, :any, virtual: true
    field :bq_table_typemap, :any, virtual: true

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
      :api_quota,
      :webhook_notification_url,
      :slack_hook_url
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
      :bigquery_table_ttl,
      :webhook_notification_url,
      :slack_hook_url
    ])
    |> default_validations()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
  end

  def generate_bq_table_id(%__MODULE__{} = source) do
    default_project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]

    bq_project_id = source.user.bigquery_project_id || default_project_id

    env = Application.get_env(:logflare, :env)
    table = GenUtils.format_table_name(source.token)

    dataset_id = source.user.bigquery_dataset_id || "#{source.user.id}_#{env}"

    "`#{bq_project_id}`.#{dataset_id}.#{table}"
  end
end
