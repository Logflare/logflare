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
    specs =
      source
      |> Backends.list_backends()
      |> Enum.map(&Backend.child_spec(source, &1))

    user = Users.Cache.get(source.user_id)
    plan = Billing.Cache.get_plan_by_user(user)

    {project_id, dataset_id} =
      if !user.bigquery_project_id do
        project_id = User.bq_project_id()
        dataset_id = User.generate_bq_dataset_id(source.user_id)
        {project_id, dataset_id}
      else
        {user.bigquery_project_id, user.bigquery_dataset_id}
      end

    default_backend =
      Backend.child_spec(source, %Backend{
        type: :bigquery,
        config: %{
          project_id: project_id,
          dataset_id: dataset_id
        }
      })

    children =
      [
        {RecentLogsServer, [source: source]},
        {RateCounterServer, [source: source]},
        {EmailNotificationServer, [source: source]},
        {TextNotificationServer, [source: source, plan: plan]},
        {WebhookNotificationServer, [source: source]},
        {SlackHookServer, [source: source]},
        {SearchQueryExecutor, [source: source]},
        {BillingWriter, [source: source]},
        default_backend
      ] ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end
end
