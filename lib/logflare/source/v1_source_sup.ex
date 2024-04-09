defmodule Logflare.Source.V1SourceSup do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.WebhookNotificationServer
  alias Logflare.Source.SlackHookServer
  alias Logflare.Source.BillingWriter
  alias Logflare.Backends.IngestEventQueue

  alias Logflare.Source.RateCounterServer, as: RCS
  alias Logflare.Users
  alias Logflare.Backends
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
      |> Users.maybe_put_bigquery_defaults()
      |> Users.preload_billing_account()

    plan = Billing.Cache.get_plan_by_user(user)

    children = [
      {RCS, [source: source]},
      {IngestEventQueue.DemandWorker, source: source, backend: nil},
      {IngestEventQueue.QueueJanitor, source: source, backend: nil},
      {Pipeline,
       [
         bigquery_project_id: user.bigquery_project_id,
         bigquery_dataset_id: user.bigquery_dataset_id,
         source: source,
         source_token: source.token,
         name: Backends.via_source(source.id, {Pipeline, nil, 0})
       ]},
      {RecentLogsServer, [source: source]},
      {Schema,
       [
         name: Backends.via_source(source, Schema, nil),
         source: source,
         bigquery_project_id: user.bigquery_project_id,
         bigquery_dataset_id: user.bigquery_dataset_id,
         plan: plan
       ]},
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
