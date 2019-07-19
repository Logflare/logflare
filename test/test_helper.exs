ExUnit.start()
Faker.start()

{:ok, _} = Application.ensure_all_started(:ex_machina)

ExUnit.configure(exclude: [integration: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
