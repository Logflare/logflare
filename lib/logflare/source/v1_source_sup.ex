defmodule Logflare.Source.V1SourceSup do
  @moduledoc false
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.RecentInsertsBroadcaster
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.WebhookNotificationServer
  alias Logflare.Source.SlackHookServer
  alias Logflare.Source.BillingWriter
  alias Logflare.GenSingleton
  alias Logflare.Source.RateCounterServer, as: RCS
  alias Logflare.Users
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Billing
  alias Logflare.Logs.SearchQueryExecutor

  require Logger
  use Supervisor

  def start_link(args) do
    source = Keyword.get(args, :source)
    Supervisor.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  @impl true
  def init(args) do
    source = Keyword.get(args, :source)
    Logger.metadata(source_id: source.token, source_token: source.token)

    user =
      source.user_id
      |> Users.Cache.get()
      |> Users.preload_billing_account()

    plan = Billing.Cache.get_plan_by_user(user)

    backend = Backends.get_default_backend(user)

    default_bigquery_spec = Backend.child_spec(source, backend)

    children = [
      {RCS, [source: source]},
      default_bigquery_spec,
      {GenSingleton, child_spec: {RecentEventsTouch, source: source}},
      {RecentInsertsBroadcaster, [source: source]},
      {EmailNotificationServer, [source: source]},
      {TextNotificationServer, [source: source, plan: plan]},
      {WebhookNotificationServer, [source: source]},
      {SlackHookServer, [source: source]},
      {SearchQueryExecutor, [source: source]},
      {BillingWriter, [source: source]}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end
end
