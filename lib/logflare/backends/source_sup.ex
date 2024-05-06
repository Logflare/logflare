defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends.Backend
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Source.RateCounterServer
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.WebhookNotificationServer
  alias Logflare.Source.SlackHookServer
  alias Logflare.Source.BillingWriter
  alias Logflare.Logs.SearchQueryExecutor

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    ingest_backends =
      source
      |> Backends.Cache.list_backends()

    rules_backends =
      source
      |> Backends.list_backends_with_rules()
      |> Enum.map(&%{&1 | register_for_ingest: false})
      |> dbg()

    user = Users.Cache.get(source.user_id)

    plan = Billing.Cache.get_plan_by_user(user)

    {project_id, dataset_id} =
      if user.bigquery_project_id do
        {user.bigquery_project_id, user.bigquery_dataset_id}
      else
        project_id = User.bq_project_id()
        dataset_id = User.generate_bq_dataset_id(source.user_id)
        {project_id, dataset_id}
      end

    specs =
      ([
         %Backend{
           type: :bigquery,
           config: %{
             project_id: project_id,
             dataset_id: dataset_id
           }
         }
         | ingest_backends
       ] ++ rules_backends)
      |> Enum.map(&Backend.child_spec(source, &1))

    children =
      [
        {RateCounterServer, [source: source]},
        {RecentLogsServer, [source: source]},
        {EmailNotificationServer, [source: source]},
        {TextNotificationServer, [source: source, plan: plan]},
        {WebhookNotificationServer, [source: source]},
        {SlackHookServer, [source: source]},
        {SearchQueryExecutor, [source: source]},
        {BillingWriter, [source: source]}
      ] ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end
end
