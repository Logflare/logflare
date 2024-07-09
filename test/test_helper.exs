Mix.Task.run("app.start")

ExUnit.start()

# Mimick mocks setup
Mimic.copy(Logflare.Google.CloudResourceManager)
Mimic.copy(Logflare.Mailer)
Mimic.copy(Logflare.Logs)
Mimic.copy(Logflare.Logs.LogEvents)
Mimic.copy(Logflare.Logs.SearchQueryExecutor)
Mimic.copy(Logflare.Lql)
Mimic.copy(Logflare.Users)
Mimic.copy(Logflare.Sources)
Mimic.copy(Logflare.Billing)
Mimic.copy(Logflare.Google.BigQuery)
Mimic.copy(Logflare.Source.RateCounterServer)
Mimic.copy(Logflare.Source.BigQuery.Schema)
Mimic.copy(Logflare.SystemMetrics.AllLogsLogged)
Mimic.copy(Logflare.Sources.Cache)
Mimic.copy(Logflare.SingleTenant)
Mimic.copy(Logflare.Backends.Adaptor.WebhookAdaptor)
Mimic.copy(Logflare.Backends.Adaptor.WebhookAdaptor.Client)
Mimic.copy(Logflare.Backends.Adaptor.SlackAdaptor.Client)
Mimic.copy(LogflareWeb.Plugs.RateLimiter)
Mimic.copy(Logflare.AlertsScheduler)
Mimic.copy(Stripe.Customer)
Mimic.copy(Stripe.PaymentMethod)
Mimic.copy(Stripe.SubscriptionItem.Usage)
Mimic.copy(GoogleApi.BigQuery.V2.Api.Jobs)
Mimic.copy(GoogleApi.BigQuery.V2.Api.Tabledata)
Mimic.copy(GoogleApi.BigQuery.V2.Api.Tables)
Mimic.copy(GoogleApi.BigQuery.V2.Api.Datasets)
Mimic.copy(GoogleApi.CloudResourceManager.V1.Api.Projects)
Mimic.copy(Goth)
Mimic.copy(ConfigCat)
Mimic.copy(Finch)
Mimic.copy(ExTwilio.Message)

{:ok, _} = Application.ensure_all_started(:ex_machina)
{:ok, _} = Application.ensure_all_started(:mimic)

# stub all outgoing requests
Mimic.stub(Goth)
Mimic.stub(Finch)

ExUnit.configure(exclude: [integration: true, failing: true, benchmark: true])

Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, :manual)
