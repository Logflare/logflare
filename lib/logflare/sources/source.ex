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
             :description,
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
             :transform_key_values,
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

  @system_source_types [:metrics, :logs]

  typed_schema "sources" do
    field :name, :string
    field :description, :string, default: nil
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
    field :transform_key_values, :string
    field :transform_key_values_parsed, {:array, :map}, virtual: true
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
      :description,
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
      :transform_key_values,
      :disable_tailing,
      :default_ingest_backend_enabled?,
      :bq_storage_write_api,
      :labels,
      :system_source,
      :system_source_type
    ])
    |> cast_embed(:notifications, with: &Notifications.changeset/2)
    |> default_validations(source)
  end

  def update_by_user_changeset(source, attrs) do
    source
    |> cast(attrs, [
      :name,
      :description,
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
      :transform_key_values,
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
    |> normalize_description()
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
    |> then(fn
      changeset when normalized == [] ->
        changeset

      changeset ->
        changeset
        |> put_change(:labels, normalized |> Enum.reverse() |> Enum.join(","))
    end)
  end

  defp normalize_description(changeset) do
    update_change(changeset, :description, fn
      description when is_binary(description) ->
        String.trim(description)

      value ->
        value
    end)
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

  @spec parse_key_values_config(%__MODULE__{}) :: %__MODULE__{}
  def parse_key_values_config(%__MODULE__{transform_key_values: nil} = source) do
    %{source | transform_key_values_parsed: nil}
  end

  def parse_key_values_config(%__MODULE__{transform_key_values: config} = source) do
    parsed =
      config
      |> String.split(~r/\n/, trim: true)
      |> Enum.flat_map(fn instruction ->
        instruction = String.trim(instruction)

        case String.split(instruction, ":", parts: 3) do
          [lookup_key, dest_key, accessor_path] ->
            lookup_key = String.replace_prefix(lookup_key, "m.", "metadata.")
            dest_key = String.replace_prefix(dest_key, "m.", "metadata.")

            [
              %{
                from_path: String.split(lookup_key, "."),
                to_path: String.split(dest_key, "."),
                accessor_path: String.trim(accessor_path)
              }
            ]

          [lookup_key, dest_key] ->
            lookup_key = String.replace_prefix(lookup_key, "m.", "metadata.")
            dest_key = String.replace_prefix(dest_key, "m.", "metadata.")

            [
              %{
                from_path: String.split(lookup_key, "."),
                to_path: String.split(dest_key, "."),
                accessor_path: nil
              }
            ]

          _ ->
            []
        end
      end)

    %{source | transform_key_values_parsed: parsed}
  end

  def system_source_types, do: @system_source_types

  @doc """
  Returns a combined list of BigQuery clustering fields
  and suggested keys to be used for query optimization.

  ## Examples

      iex> source = %Source{bigquery_clustering_fields: "id,timestamp", suggested_keys: "m.user_id,status"}
      iex> recommended_query_fields(source)
      ["id", "timestamp", "m.user_id", "status"]


    with trailing `!` for `:suggested_keys`:

      iex> source = %Source{bigquery_clustering_fields: "id,timestamp", suggested_keys: "m.user_id!,status"}
      iex> recommended_query_fields(source)
      ["id", "timestamp", "m.user_id!", "status"]


      iex> source = %Source{bigquery_clustering_fields: nil, suggested_keys: ""}
      iex> recommended_query_fields(source)
      []
  """
  @spec recommended_query_fields(%__MODULE__{}) :: [String.t()]
  def recommended_query_fields(%__MODULE__{} = source) do
    clustering_fields =
      (source.bigquery_clustering_fields || "")
      |> String.split(",", trim: true)

    suggested_keys =
      (source.suggested_keys || "")
      |> String.split(",", trim: true)
      |> Enum.map(&String.trim/1)

    clustering_fields ++ suggested_keys
  end

  @spec required_query_field?(String.t()) :: boolean()
  def required_query_field?(field) when is_binary(field) do
    field
    |> String.trim()
    |> String.ends_with?("!")
  end

  @spec query_field_name(String.t()) :: String.t()
  def query_field_name(field) when is_binary(field) do
    field
    |> String.trim()
    |> String.trim_trailing("!")
  end
end
