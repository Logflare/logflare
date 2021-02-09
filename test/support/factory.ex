defmodule Logflare.Factory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo
  use Logflare.Commons

  def user_factory do
    %User{
      name: Faker.Person.name(),
      email: Faker.Internet.email(),
      provider: "google",
      bigquery_processed_bytes_limit: 10_000_000_000,
      token: Faker.String.base64(64),
      api_key: Faker.String.base64(10),
      preferences: build(:user_preferences),
      provider_uid: sequence(:provider_uid, &"uid_#{&1}"),
      bigquery_udfs_hash: "0000000"
    }
  end

  def user_with_iam() do
    email = System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM")
    bigquery_dataset_location = "US"
    # bigquery_project_id = "logflare-dev-238720"
    # bigquery_table_ttl = 60 * 60 * 24 * 365
    source_token = "2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

    {:ok, u} =
      Users.insert_or_update_user(%{
        id: 314_159,
        email: email,
        bigquery_dataset_location: bigquery_dataset_location,
        provider: "google",
        provider_uid: "000000",
        token: "token",
        api_key: "api_key",
        name: "Test user name"
      })

    u
  end

  def source_factory do
    %Source{
      name: Faker.Superhero.name(),
      token: Faker.UUID.v4(),
      rules: [],
      favorite: false,
      source_schema: build(:source_schema)
    }
  end

  def source_schema_factory() do
    %SourceSchema{}
  end

  def notifications_factory do
    %Source.Notifications{}
  end

  def rule_factory do
    %Rule{}
  end

  def log_event_factory(attrs) do
    {source, params} = Map.pop(attrs, :source)

    params = %{
      "message" => params["message"] || params[:message] || "test-msg",
      "timestamp" => params["timestamp"] || params[:timestamp] || DateTime.utc_now() |> to_string,
      "metadata" => params["metadata"] || params[:metadata] || %{}
    }

    LogEvent.make(params, %{source: source})
  end

  def plan_factory() do
    %Plan{
      stripe_id: "31415"
    }
  end

  def user_preferences_factory() do
    %UserPreferences{
      timezone: "Phoenix/Arizona"
    }
  end
end
