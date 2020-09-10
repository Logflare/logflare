ExUnit.start()
Faker.start()

# Mimick mocks setup
Mimic.copy(Logflare.Logs.LogEvents)
Mimic.copy(Logflare.Logs.SearchQueryExecutor)
Mimic.copy(Logflare.Lql)

{:ok, _} = Application.ensure_all_started(:ex_machina)
{:ok, _} = Application.ensure_all_started(:mimic)

ExUnit.configure(exclude: [integration: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
