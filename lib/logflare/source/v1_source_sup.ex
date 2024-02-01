defmodule Logflare.Source.V1SourceSup do
  @moduledoc """
  Manages the individual table for the source. Limits things in the table to 1000. Manages TTL for
  things in the table. Handles loading the table from the disk if found on startup.
  """

  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.BufferCounter

  alias Logflare.Source.RecentLogsServer
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.WebhookNotificationServer
  alias Logflare.Source.SlackHookServer
  alias Logflare.Source.BillingWriter

  alias Logflare.Source.RateCounterServer, as: RCS
  alias Logflare.Source
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.Logs.SearchQueryExecutor

  require Logger
  use Supervisor

  def start_link(%RecentLogsServer{source_id: source_token} = rls) do
    Supervisor.start_link(__MODULE__, rls, name: Source.Supervisor.via(__MODULE__, source_token))
  end

  @impl true
  def init(%RecentLogsServer{source: source} = rls) do
    Process.flag(:trap_exit, true)
    Logger.metadata(source_id: rls.source_id, source_token: rls.source_id)

    user =
      source.user_id
      |> Users.get()
      |> Users.maybe_put_bigquery_defaults()
      |> Users.preload_billing_account()

    plan = Billing.get_plan_by_user(user)

    rls = %RecentLogsServer{
      rls
      | bigquery_project_id: user.bigquery_project_id,
        bigquery_dataset_id: user.bigquery_dataset_id,
        user: user,
        plan: plan,
        notifications_every: source.notifications_every
    }

    children = [
      {BufferCounter, rls},
      {Pipeline, rls},
      {RecentLogsServer, rls},
      {Schema, rls},
      {RCS, rls},
      {EmailNotificationServer, rls},
      {TextNotificationServer, rls},
      {WebhookNotificationServer, rls},
      {SlackHookServer, rls},
      {SearchQueryExecutor, rls},
      {BillingWriter, rls}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end
end
