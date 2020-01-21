defmodule Logflare.User do
  @moduledoc """
  User schema and changeset
  """
  use TypedEctoSchema
  import Ecto.Changeset
  @default_user_api_quota 150

  alias Logflare.Source
  alias Logflare.Teams.Team
  alias Logflare.Google.BigQuery

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]
  @default_dataset_location "US"

  typed_schema "users" do
    field :email, :string
    field :provider, :string
    field :token, :string
    field :api_key, :string
    field :old_api_key, :string
    field :email_preferred, :string
    field :name, :string
    field :image, :string
    field :email_me_product, :boolean, default: true
    field :admin, :boolean, default: false
    has_many :sources, Source
    has_one :team, Team
    field :phone, :string
    field :bigquery_project_id, :string
    field :bigquery_dataset_location, :string
    field :bigquery_dataset_id, :string
    field :api_quota, :integer, default: @default_user_api_quota
    field :valid_google_account, :boolean
    field :provider_uid, :string
    field :company, :string

    timestamps()
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, [
      :email,
      :provider,
      :token,
      :api_key,
      :old_api_key,
      :email_preferred,
      :name,
      :image,
      :email_me_product,
      # Don't cast this so people can't hack into admin mode
      # :admin,
      :phone,
      :bigquery_project_id,
      :api_quota,
      :bigquery_dataset_location,
      :bigquery_dataset_id,
      :valid_google_account,
      :provider_uid,
      :company
    ])
    |> cast_assoc(:team)
    |> default_validations(user)
  end

  def default_validations(changeset, user) do
    changeset
    |> validate_required([:email, :provider, :token, :provider_uid])
    |> validate_bq_dataset_location()
    |> validate_gcp_project(:bigquery_project_id, user_id: user.id)
  end

  def validate_gcp_project(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, bigquery_project_id ->
      user_id = Integer.to_string(options[:user_id])

      dataset_id =
        changeset.changes[:bigquery_dataset_id] || "#{options[:user_id]}" <> @dataset_id_append

      location = changeset.changes[:bigquery_dataset_location] || @default_dataset_location

      project_id = bigquery_project_id || @project_id

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
    ~w(US EU us-west2 northamerica-northeast1 us-east4 southamerica-east1 europe-north1 europe-west2 europe-west6 asia-east2 asia-south1 asia-northeast2 asia-east1 asia-northeast1 asia-southeast1 australia-southeast1)
  end

  def generate_bq_dataset_id(%__MODULE__{id: id} = user) do
    "#{id}" <> @dataset_id_append
  end
end
