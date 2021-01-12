ExUnit.start()
Faker.start()
use Logflare.Commons

# Mimic mocks setup
Mimic.copy(Logs.LogEvents)
Mimic.copy(Logs.SearchQueryExecutor)
Mimic.copy(Lql)
Mimic.copy(Plans)
Mimic.copy(Sources.Counters)
Mimic.copy(Sources)
Mimic.copy(Source.Supervisor)
Mimic.copy(Logflare.Google.BigQuery)

{:ok, _} = Application.ensure_all_started(:ex_machina)
{:ok, _} = Application.ensure_all_started(:mimic)

ExUnit.configure(exclude: [integration: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
