ExUnit.start()
Faker.start()

:ok = LocalCluster.start()

{:ok, _} = Application.ensure_all_started(:ex_machina)

{:ok, _} = Application.ensure_all_started(:logflare)

ExUnit.configure(exclude: [integration: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
