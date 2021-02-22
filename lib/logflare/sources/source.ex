defmodule Logflare.Source do
  @moduledoc false
  use TypedEctoSchema
  use Logflare.Commons
  use Logflare.Changefeeds.ChangefeedSchema, derive_virtual: [:bq_table_id, :bq_dataset_id]

  import Ecto.Changeset

  @default_source_api_quota 25
  @derive {Jason.Encoder,
           only: [
             :name,
             :token,
             :id,
             :favorite,
             :webhook_notification_url,
             :api_quota,
             :slack_hook_url,
             :bigquery_table_ttl,
             :public_token,
             :bq_table_id,
             :has_rejected_events,
             :metrics,
             :notifications,
             :custom_event_message_keys
           ]}
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  defmodule Metrics do
    @moduledoc false
    use TypedEctoSchema

    @derive {Jason.Encoder,
             only: [
               :rate,
               :latest,
               :avg,
               :max,
               :buffer,
               :inserts,
               :inserts_string,
               :recent,
               :rejected,
               :fields
             ]}

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

  defmodule Notifications do
    @moduledoc false
    use Ecto.Schema
    @primary_key false
    @derive {Jason.Encoder,
             only: [
               :team_user_ids_for_email,
               :team_user_ids_for_sms,
               :other_email_notifications,
               :user_email_notifications,
               :user_text_notifications,
               :user_schema_update_notifications,
               :team_user_ids_for_schema_updates
             ]}

    typed_embedded_schema do
      field :team_user_ids_for_email, {:array, :string}, default: [], nullable: false
      field :team_user_ids_for_sms, {:array, :string}, default: [], nullable: false
      field :other_email_notifications, :string
      field :user_email_notifications, :boolean, default: false
      field :user_text_notifications, :boolean, default: false
      field :user_schema_update_notifications, :boolean, default: true
      field :team_user_ids_for_schema_updates, {:array, :string}, default: [], nullable: false
    end

    def changeset(notifications, attrs) do
      notifications
      |> cast(attrs, [
        :team_user_ids_for_email,
        :team_user_ids_for_sms,
        :other_email_notifications,
        :user_email_notifications,
        :user_text_notifications,
        :user_schema_update_notifications,
        :team_user_ids_for_schema_updates
      ])
    end
  end

  typed_schema "sources" do
    field :name, :string
    field :token, Ecto.UUID.Atom
    field :public_token, :string
    field :favorite, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota
    field :webhook_notification_url, :string
    field :slack_hook_url, :string
    field :metrics, :map, virtual: true, default: %{}
    field :has_rejected_events, :boolean, default: false, virtual: true
    field :bq_table_id, :string, virtual: true
    field :bq_dataset_id, :string, virtual: true
    # field :bq_table_typemap, :map, virtual: true
    field :bq_table_partition_type, Ecto.Enum, values: [:pseudo, :timestamp], default: :timestamp
    field :custom_event_message_keys, :string
    field :log_events_updated_at, :naive_datetime
    field :notifications_every, :integer, default: :timer.hours(4), nullable: false

    # Causes a shitstorm
    # field :bigquery_schema, Ecto.Term

    belongs_to :user, Logflare.User

    has_many :rules, Logflare.Rule
    has_many :saved_searches, Logflare.SavedSearch
    has_many :billing_counts, Logflare.BillingCounts.BillingCount

    embeds_one :notifications, Notifications, on_replace: :update

    has_one :source_schema, SourceSchema

    timestamps()
  end

  def changefeed_changeset(struct \\ struct(__MODULE__), attrs) do
    chgst = EctoChangesetExtras.cast_all_fields(struct, attrs)

    cast_embed(chgst, :notifications, with: &Notifications.changeset/2)
  end

  @default_table_name_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] ||
                               ""

  def derive(:bq_table_id, %__MODULE__{} = source, _virtual_struct_params) do
    user = Users.get_user(source.user_id)
    generate_bq_table_id(%{source | user: user})
  end

  def derive(:bq_dataset_id, %__MODULE__{} = source, _virtual_struct_params) do
    user = Users.get_user(source.user_id)
    user.bigquery_dataset_id || "#{source.user_id}" <> @default_table_name_append
  end

  def no_casting_changeset(source) do
    source
    |> cast(%{}, [])
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :favorite,
      :bigquery_table_ttl,
      # users can't update thier API quota currently
      :api_quota,
      :webhook_notification_url,
      :slack_hook_url,
      :custom_event_message_keys,
      :log_events_updated_at,
      :notifications_every
    ])
    |> cast_embed(:notifications, with: &Notifications.changeset/2)
    |> default_validations(source)
  end

  def update_by_user_changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :token,
      :public_token,
      :favorite,
      :bigquery_table_ttl,
      :webhook_notification_url,
      :slack_hook_url,
      :custom_event_message_keys,
      :notifications_every
    ])
    |> cast_embed(:notifications, with: &Notifications.changeset/2)
    |> default_validations(source)
  end

  def default_validations(changeset, source) do
    changeset
    |> validate_required([:name, :token])
    |> unique_constraint(:name, name: :sources_name_index)
    |> unique_constraint(:token)
    |> unique_constraint(:public_token)
    |> validate_source_ttl(source)
  end

  def validate_source_ttl(changeset, source) do
    if source.user_id do
      user = Users.get_user!(source.user_id)
      plan = Plans.get_plan_by_user(user)

      validate_change(changeset, :bigquery_table_ttl, fn :bigquery_table_ttl, ttl ->
        days = round(plan.limit_source_ttl / :timer.hours(24))

        cond do
          user.bigquery_project_id ->
            []

          ttl > days ->
            [bigquery_table_ttl: "ttl is over your plan limit"]

          true ->
            []
        end
      end)
    else
      changeset
    end
  end

  def generate_bq_table_id(%__MODULE__{user: %User{} = user} = source) do
    default_project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]

    bq_project_id = source.user.bigquery_project_id || default_project_id

    table = format_table_name(source.token)

    dataset_id = source.user.bigquery_dataset_id || "#{source.user.id}" <> @dataset_id_append

    "`#{bq_project_id}`.#{dataset_id}.#{table}"
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source_token) when is_atom(source_token) do
    source_token
    |> Atom.to_string()
    |> String.replace("-", "_")
  end
end
