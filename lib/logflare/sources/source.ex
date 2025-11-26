defmodule Logflare.Sources.Source do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Billing
  alias Logflare.Users

  @default_source_api_quota 25
  @derive {Jason.Encoder,
           only: [
             :name,
             :service_name,
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
             :custom_event_message_keys,
             :backends,
             :retention_days,
             :transform_copy_fields,
             :bigquery_clustering_fields,
             :default_ingest_backend_enabled?
           ]}

  defp env_dataset_id_append,
    do: Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

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
      field(:rate, :integer)
      field(:latest, :integer)
      field(:avg, :integer)
      field(:max, :integer)
      field(:buffer, :integer)
      field(:inserts, :integer)
      field(:inserts_string, :string)
      field(:recent, :integer)
      field(:rejected, :integer)
      field(:fields, :integer)
    end
  end

  defmodule Notifications do
    @moduledoc false
    use TypedEctoSchema

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
      field(:team_user_ids_for_email, {:array, :string}, default: [])
      field(:team_user_ids_for_sms, {:array, :string}, default: [])
      field(:other_email_notifications, :string)
      field(:user_email_notifications, :boolean, default: false)
      field(:user_text_notifications, :boolean, default: false)
      field(:user_schema_update_notifications, :boolean, default: true)
      field(:team_user_ids_for_schema_updates, {:array, :string}, default: [])
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

  @system_source_types [:rejected_events, :metrics, :logs]

  typed_schema "sources" do
    field :name, :string
    field :service_name, :string
    field :token, Ecto.UUID.Atom, autogenerate: true
    field :public_token, :string
    field :favorite, :boolean, default: false
    field :bigquery_table_ttl, :integer
    field :api_quota, :integer, default: @default_source_api_quota
    field :webhook_notification_url, :string
    field :slack_hook_url, :string
    field :metrics, :map, virtual: true
    field :has_rejected_events, :boolean, default: false, virtual: true
    field :bq_table_id, :string, virtual: true
    field :bq_dataset_id, :string, virtual: true
    field :bq_table_partition_type, Ecto.Enum, values: [:pseudo, :timestamp], default: :timestamp
    field :bq_storage_write_api, :boolean, default: false
    field :custom_event_message_keys, :string
    field :log_events_updated_at, :naive_datetime
    field :notifications_every, :integer, default: :timer.hours(4)
    field :lock_schema, :boolean, default: false
    field :validate_schema, :boolean, default: true
    field :drop_lql_filters, Ecto.LqlRules, default: []
    field :drop_lql_string, :string
    field :disable_tailing, :boolean, default: false
    field :suggested_keys, :string, default: ""
    field :retention_days, :integer, virtual: true
    field :transform_copy_fields, :string
    field :bigquery_clustering_fields, :string
    field :system_source, :boolean, default: false
    field :system_source_type, Ecto.Enum, values: @system_source_types
    field :labels, :string

    field :default_ingest_backend_enabled?, :boolean,
      source: :default_ingest_backend_enabled,
      default: false

    # Causes a shitstorm
    # field :bigquery_schema, Ecto.Term

    belongs_to :user, Logflare.User

    has_many :rules, Logflare.Rules.Rule

    many_to_many :backends, Logflare.Backends.Backend,
      join_through: "sources_backends",
      on_replace: :delete

    has_many :saved_searches, Logflare.SavedSearch
    has_many :billing_counts, Logflare.Billing.BillingCount, on_delete: :nothing

    embeds_one :notifications, Notifications, on_replace: :update

    has_one :source_schema, Logflare.SourceSchemas.SourceSchema

    timestamps()
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
      :service_name,
      :token,
      :public_token,
      :favorite,
      :bigquery_table_ttl,
      :bigquery_clustering_fields,
      # users can't update thier API quota currently
      :api_quota,
      :webhook_notification_url,
      :slack_hook_url,
      :custom_event_message_keys,
      :log_events_updated_at,
      :notifications_every,
      :lock_schema,
      :validate_schema,
      :drop_lql_filters,
      :drop_lql_string,
      :suggested_keys,
      :retention_days,
      :transform_copy_fields,
      :disable_tailing,
      :default_ingest_backend_enabled?,
      :bq_storage_write_api,
      :labels
    ])
    |> cast_embed(:notifications, with: &Notifications.changeset/2)
    |> default_validations(source)
  end

  def update_by_user_changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :service_name,
      :token,
      :public_token,
      :favorite,
      :bigquery_table_ttl,
      :bigquery_clustering_fields,
      :webhook_notification_url,
      :slack_hook_url,
      :custom_event_message_keys,
      :notifications_every,
      :lock_schema,
      :validate_schema,
      :drop_lql_filters,
      :drop_lql_string,
      :suggested_keys,
      :retention_days,
      :transform_copy_fields,
      :disable_tailing,
      :default_ingest_backend_enabled?,
      :bq_storage_write_api,
      :labels
    ])
    |> cast_embed(:notifications, with: &Notifications.changeset/2)
    |> default_validations(source)
  end

  def default_validations(changeset, source) do
    changeset
    |> validate_required([:name])
    |> unique_constraint(:name, name: :sources_name_index)
    |> unique_constraint(:token)
    |> unique_constraint(:public_token)
    |> put_source_ttl_change()
    |> validate_source_ttl(source)
    |> normalize_and_validate_labels()
  end

  defp put_source_ttl_change(changeset) do
    value = get_field(changeset, :retention_days)
    put_change(changeset, :bigquery_table_ttl, value)
  end

  def validate_source_ttl(changeset, source) do
    if source.user_id do
      user = Users.get(source.user_id)
      plan = Billing.get_plan_by_user(user)

      validate_change(changeset, :bigquery_table_ttl, fn :bigquery_table_ttl, ttl ->
        days = round(plan.limit_source_ttl / :timer.hours(24))

        cond do
          user.bigquery_project_id != Application.get_env(:logflare, Logflare.Google)[:project_id] ->
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

  defp normalize_and_validate_labels(changeset) do
    {normalized, errors} =
      case get_change(changeset, :labels) do
        value when value in [nil, ""] ->
          {[], []}

        labels ->
          get_normalized_and_errors(labels)
      end

    errors
    |> Enum.uniq()
    |> Enum.reduce(changeset, fn {k, v}, cs -> add_error(cs, k, v) end)
    |> put_change(:labels, normalized |> Enum.reverse() |> Enum.join(","))
  end

  defp get_normalized_and_errors(labels) do
    labels
    |> String.split(",", trim: true)
    |> Enum.reduce({[], []}, fn pair, {normalized, errors} ->
      case pair |> String.trim() |> String.split("=") do
        [k, v] when k != "" and v != "" ->
          {["#{String.trim(k)}=#{String.trim(v)}" | normalized], errors}

        [_, ""] ->
          {normalized, [{:labels, "each label must have a non-empty value"} | errors]}

        ["", _] ->
          {normalized, [{:labels, "each label must have a non-empty key"} | errors]}

        [_] ->
          {normalized, [{:labels, "each label must be in key=value format"} | errors]}

        _ ->
          {normalized, [{:labels, "each label must have exactly one '=' sign"} | errors]}
      end
    end)
  end

  def generate_bq_table_id(%__MODULE__{} = source) do
    default_project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]

    bq_project_id = source.user.bigquery_project_id || default_project_id

    table = format_table_name(source.token)

    dataset_id = source.user.bigquery_dataset_id || "#{source.user.id}" <> env_dataset_id_append()

    "`#{bq_project_id}`.#{dataset_id}.#{table}"
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source_token) when is_atom(source_token) do
    source_token
    |> Atom.to_string()
    |> String.replace("-", "_")
  end

  def system_source_types, do: @system_source_types
end
