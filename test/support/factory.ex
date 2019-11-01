defmodule Logflare.Factory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo
  alias Logflare.{User, Source, Rule, LogEvent}

  def user_factory do
    %User{
      name: "JaneJohn Jones",
      email: sequence(:email, &"email-#{&1}@example.com"),
      provider: "google",
      token: Faker.String.base64(64),
      api_key: Faker.String.base64(10),
      provider_uid: "provider_uid"
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
    %Rule{regex: "."}
  end

  def log_event_factory(attrs) do
    {source, params} = Map.pop(attrs, :source)

    params = %{
      "message" => params["message"] || params[:message] || "test-msg",
      "timestamp" => params["timestamp"] || params[:timestamp] || DateTime.utc_now() |> to_string
    }

    LogEvent.make(params, %{source: source})
  end
end
