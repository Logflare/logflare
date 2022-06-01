defmodule Logflare.Factory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo
  alias Logflare.{User, Source, Rule, LogEvent}
  alias Logflare.Users.UserPreferences
  alias Logflare.Endpoint.Query
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.{Plans.Plan, Teams.Team, TeamUsers.TeamUser}

  def user_factory do
    %User{
      name: "JaneJohn Jones",
      email: Faker.Internet.email(),
      provider: "google",
      bigquery_processed_bytes_limit: 10_000_000_000,
      token: Faker.String.base64(64),
      api_key: Faker.String.base64(10),
      provider_uid: "provider_uid",
      bigquery_udfs_hash: ""
    }
  end

  def team_factory do
    %Team{
      name: "my team #{random_string()}",
      user: build(:user)
    }
  end

  def team_user_factory do
    %TeamUser{
      name: "some name #{random_string()}",
      team: build(:team)
    }
  end

  def source_factory do
    %Source{
      name: Faker.Superhero.name(),
      token: Faker.UUID.v4(),
      rules: [],
      favorite: false
    }
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

  def endpoint_factory do
    %Query{
      user: build(:user),
      token: Ecto.UUID.generate()
    }
  end

  def access_token_factory do
    %OauthAccessToken{
      token: random_string(20),
      resource_owner: build(:user)
    }
  end

  defp random_string(length \\ 6) do
    :crypto.strong_rand_bytes(length) |> Base.url_encode64() |> binary_part(0, length)
  end
end
