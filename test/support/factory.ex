defmodule Logflare.Factory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo
  alias Logflare.{User, Source, Rule, LogEvent}

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
end
