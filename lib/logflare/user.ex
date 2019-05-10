defmodule Logflare.User do
  use Ecto.Schema
  import Ecto.Changeset
  @default_user_api_quota 1000

  alias Logflare.Google.BigQuery

  schema "users" do
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
    has_many :sources, Logflare.Source
    field :phone, :string
    field :bigquery_project_id, :string
    field :api_quota, :integer, default: @default_user_api_quota

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
      :admin,
      :phone,
      :bigquery_project_id,
      :api_quota
    ])
    |> validate_required([:email, :provider, :token])
    |> validate_gcp_project(:bigquery_project_id, user_id: user.id)
  end

  def validate_gcp_project(changeset, field, options \\ []) do
    validate_change(changeset, field, fn _, bigquery_project_id ->
      case BigQuery.create_dataset(Integer.to_string(options[:user_id]), bigquery_project_id) do
        {:ok, _} ->
          []

        {:error, %Tesla.Env{status: 409}} ->
          []

        {:error, _message} ->
          [{field, options[:message] || "Check your GCP permissions!"}]
      end
    end)
  end
end
