defmodule Logflare.User do
  @moduledoc """
  User schema and changeset
  """
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Sources.Source
  alias Logflare.Teams.Team
  alias Logflare.Billing.BillingAccount
  alias Logflare.Google.BigQuery
  alias Logflare.Users.UserPreferences
  alias Logflare.Vercel
  alias Logflare.Partners.Partner
  alias Logflare.Alerting.AlertQuery

  @derive {Jason.Encoder,
           only: [
             :email,
             :provider,
             :api_key,
             :email_preferred,
             :name,
             :image,
             :email_me_product,
             :phone,
             :bigquery_project_id,
             :bigquery_dataset_location,
             :bigquery_dataset_id,
             :bigquery_reservation_alerts,
             :bigquery_reservation_search,
             :api_quota,
             :company,
             :token,
             :metadata,
             :partner_upgraded
           ]}

  @default_user_api_quota 150

  @default_dataset_location "US"
  @valid_bq_dataset_locations [
    "US",
    "EU",
    "us-west2",
    "northamerica-northeast1",
    "us-east4",
    "southamerica-east1",
    "europe-north1",
    "europe-west2",
    "europe-west6",
    "asia-east2",
    "asia-south1",
    "asia-northeast2",
    "asia-east1",
    "asia-northeast1",
    "asia-southeast1",
    "australia-southeast1"
  ]

  defp env_dataset_id_append,
    do: Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  typed_schema "users" do
    field :email, :string
    field :provider, :string
    # TODO: change type to Ecto.UUID and autogenerate
    field :token, :string
    field :api_key, :string
    field :old_api_key, :string
    field :email_preferred, :string
    field :name, :string
    field :image, :string
    field :email_me_product, :boolean, default: false
    field :admin, :boolean, default: false
    field :phone, :string
    field :bigquery_project_id, :string
    field :bigquery_dataset_location, :string
    field :bigquery_dataset_id, :string
    field :bigquery_reservation_search, :string
    field :bigquery_reservation_alerts, :string
    field :bigquery_processed_bytes_limit, :integer
    field :bigquery_enable_managed_service_accounts, :boolean, default: false
    field :api_quota, :integer, default: @default_user_api_quota
    field :valid_google_account, :boolean
    field :provider_uid, :string
    field :company, :string
    field :billing_enabled, :boolean, default: true
    field :endpoints_beta, :boolean, default: false
    field :metadata, :map
    embeds_one :preferences, UserPreferences
    field :partner_upgraded, :boolean, default: false

    has_many :billing_counts, Logflare.Billing.BillingCount
    has_many :sources, Source
    has_many :endpoint_queries, Logflare.Endpoints.Query
    has_many :alert_queries, AlertQuery
    has_many :vercel_auths, Vercel.Auth

    has_one :team, Team
    belongs_to :partner, Partner
    has_one :billing_account, BillingAccount

    timestamps()
  end

  @user_allowed_fields [
    :email,
    :provider,
    :email_preferred,
    :name,
    :image,
    :email_me_product,
    :phone,
    :bigquery_project_id,
    :bigquery_dataset_location,
    :bigquery_dataset_id,
    :bigquery_reservation_alerts,
    :bigquery_reservation_search,
    :bigquery_processed_bytes_limit,
    :bigquery_enable_managed_service_accounts,
    :valid_google_account,
    :provider_uid,
    :company,
    :partner_upgraded
  ]

  @fields @user_allowed_fields ++
            [
              :metadata,
              :token,
              :api_key,
              :old_api_key,
              :api_quota,
              :billing_enabled,
              :endpoints_beta,
              :partner_id
            ]

  @doc """
  Users are not allowed to modify :token, :admin, :api_quota, :api_key and others
  """
  def user_allowed_changeset(user, attrs) do
    user
    |> hide_bigquery_defaults()
    |> cast(attrs, @user_allowed_fields)
    |> cast_assoc(:team)
    |> default_validations(user)
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, @fields)
    |> add_api_key()
    |> add_token()
    |> add_provider_uid()
    |> cast_assoc(:team)
    |> default_validations(user)
  end

  defp add_token(%{data: %{token: nil}, changes: changes} = changeset)
       when not is_map_key(changes, :token) do
    put_change(changeset, :token, Ecto.UUID.generate())
  end

  defp add_token(changeset), do: changeset

  defp add_provider_uid(%{data: %{provider_uid: nil}, changes: changes} = changeset)
       when not is_map_key(changes, :provider_uid) do
    put_change(changeset, :provider_uid, Ecto.UUID.generate())
  end

  defp add_provider_uid(changeset), do: changeset

  defp add_api_key(%{data: %{api_key: nil}, changes: changes} = changeset)
       when not is_map_key(changes, :api_key) do
    api_key =
      :crypto.strong_rand_bytes(12)
      |> Base.url_encode64()
      |> binary_part(0, 12)

    put_change(changeset, :api_key, api_key)
  end

  defp add_api_key(changeset), do: changeset

  def default_validations(changeset, user) do
    changeset
    |> validate_required([:email, :provider, :token, :provider_uid, :api_key])
    |> update_change(:email, &String.downcase/1)
    |> update_change(:email_preferred, fn
      nil -> nil
      x when is_binary(x) -> String.downcase(x)
    end)
    |> downcase_email_provider_uid(user)
    |> unique_constraint(:email, name: :users_lower_email_index)
    |> validate_bq_dataset_location()
    |> validate_gcp_project(:bigquery_project_id, user_id: user.id)
  end

  def hide_bigquery_defaults(user) do
    if user.bigquery_project_id == bq_project_id() do
      %{
        user
        | bigquery_project_id: nil,
          bigquery_dataset_id: nil,
          bigquery_dataset_location: nil,
          bigquery_processed_bytes_limit: nil,
          bigquery_reservation_alerts: nil,
          bigquery_reservation_search: nil
      }
    else
      user
    end
  end

  def downcase_email_provider_uid(changeset, user) do
    if user.provider == "email" do
      changeset
      |> update_change(:provider_uid, &String.downcase/1)
    else
      changeset
    end
  end

  def validate_gcp_project(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, bigquery_project_id ->
      user_id = Integer.to_string(options[:user_id])

      dataset_id =
        changeset.changes[:bigquery_dataset_id] ||
          "#{options[:user_id]}" <> env_dataset_id_append()

      location = changeset.changes[:bigquery_dataset_location] || @default_dataset_location

      project_id = bigquery_project_id || bq_project_id()

      case BigQuery.create_dataset(
             user_id,
             dataset_id,
             location,
             project_id
           ) do
        {:ok, _} ->
          []

        {:error, %Tesla.Env{status: 409}} ->
          []

        {:error, message} ->
          error_message = BigQuery.GenUtils.get_tesla_error_message(message)
          [{field, options[:message] || "#{error_message}"}]
      end
    end)
  end

  def validate_bq_dataset_location(changeset) do
    field = :bigquery_dataset_location

    validate_change(changeset, field, fn _, bq_dataset_loc ->
      if bq_dataset_loc in valid_bq_dataset_locations() do
        []
      else
        [{field, "Invalid BigQuery dataset location."}]
      end
    end)
  end

  def valid_bq_dataset_locations do
    @valid_bq_dataset_locations
  end

  def bq_project_id do
    Application.get_env(:logflare, Logflare.Google)[:project_id]
  end

  def generate_bq_dataset_id(%__MODULE__{id: id}), do: generate_bq_dataset_id(id)

  def generate_bq_dataset_id(id) do
    "#{id}" <> env_dataset_id_append()
  end
end
