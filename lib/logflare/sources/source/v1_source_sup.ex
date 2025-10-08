defmodule Logflare.Sources.Source.V1SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.RecentInsertsCacher
  alias Logflare.Billing
  alias Logflare.GenSingleton
  alias Logflare.Sources.Source.BillingWriter
  alias Logflare.Sources.Source.EmailNotificationServer
  alias Logflare.Sources.Source.RateCounterServer, as: RCS
  alias Logflare.Sources.Source.SlackHookServer
  alias Logflare.Sources.Source.TextNotificationServer
  alias Logflare.Sources.Source.WebhookNotificationServer
  alias Logflare.Users

  require Logger

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
      {RecentInsertsCacher, [source: source]},
      {EmailNotificationServer, [source: source]},
      {TextNotificationServer, [source: source, plan: plan]},
      {WebhookNotificationServer, [source: source]},
      {SlackHookServer, [source: source]},
      {BillingWriter, [source: source]}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end
end
