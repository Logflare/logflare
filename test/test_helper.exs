ExUnit.start()
Faker.start()
alias Logflare.{Lql, Plans, Logs, Sources}

# Mimick mocks setup
Mimic.copy(Logs.LogEvents)
Mimic.copy(Logs.SearchQueryExecutor)
Mimic.copy(Lql)
Mimic.copy(Plans)
Mimic.copy(Plans.Cache)
Mimic.copy(Sources.Counters)
Mimic.copy(Sources.Cache)
Mimic.copy(Stripe.PaymentMethod)
Mimic.copy(Logflare.SQL)

{:ok, _} = Application.ensure_all_started(:ex_machina)
{:ok, _} = Application.ensure_all_started(:mimic)

ExUnit.configure(exclude: [integration: true, failing: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
