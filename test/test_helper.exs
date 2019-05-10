ExUnit.start()
Faker.start()

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :auto)

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
