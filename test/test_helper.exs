ExUnit.start()
Faker.start()

{:ok, _} = Application.ensure_all_started(:ex_machina)
Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :auto)

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
