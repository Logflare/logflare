defmodule Logflare.DummyFactory do
  @moduledoc """
  Generates fixtures for schemas
  """
  use ExMachina.Ecto, repo: Logflare.Repo
  alias Logflare.{User, Source, Rule}

  def user_factory do
    %User{
      name: "JaneJohn Jones",
      email: sequence(:email, &"email-#{&1}@example.com"),
      provider: "google",
      token: Faker.String.base64(64),
      api_key: Faker.String.base64(10)
    }
  end

  def source_factory do
    %Source{
      name: Faker.Superhero.name(),
      token: Faker.UUID.v4(),
      favorite: false
    }
  end

  def rule_factory do
    %Rule{
    }
  end
end
