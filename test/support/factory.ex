defmodule Logflare.DummyFactory do
  use ExMachina.Ecto, repo: Logflare.Repo
  alias Logflare.{User, Source}

  def user_factory do
    %User{
      name: "JaneJohn Jones",
      email: sequence(:email, &"email-#{&1}@example.com")
    }
  end

  def source_factory do
    %Source{
      token: Faker.UUID.v4()
    }
  end
end
