defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSupWorker
  alias Logflare.Backends
  alias Logflare.Source
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.GenSingleton
  alias Logflare.Source.RateCounterServer
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.WebhookNotificationServer
  alias Logflare.Source.SlackHookServer
  alias Logflare.Source.BillingWriter
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Backends.RecentEventsTouch
  alias Logflare.Backends.RecentInsertsBroadcaster
  alias Logflare.Rule
  alias Logflare.Sources
  alias Logflare.Backends.AdaptorSupervisor

  def start_link(%Source{} = source) do
    Supervisor.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    ingest_backends = Backends.Cache.list_backends(source_id: source.id)

    rules_backends =
      Backends.Cache.list_backends(rules_source_id: source.id)
      |> Enum.map(&%{&1 | register_for_ingest: false})

    user = Users.Cache.get(source.user_id)

    plan = Billing.Cache.get_plan_by_user(user)

    default_backend = Backends.get_default_backend(user)

    specs =
      [default_backend | ingest_backends]
      |> Enum.concat(rules_backends)
      |> Enum.map(&Backend.child_spec(source, &1))
      |> Enum.uniq()

    children =
      [
        {RateCounterServer, [source: source]},
        {GenSingleton, child_spec: {RecentEventsTouch, source: source}},
        {RecentInsertsBroadcaster, [source: source]},
        {EmailNotificationServer, [source: source]},
        {TextNotificationServer, [source: source, plan: plan]},
        {WebhookNotificationServer, [source: source]},
        {SlackHookServer, [source: source]},
        {SearchQueryExecutor, [source: source]},
        {BillingWriter, [source: source]},
        {SourceSupWorker, [source: source]}
      ] ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Checks if a rule child is started for a given source/rule.
  Must be a backend rule.
  """
  @spec rule_child_started?(Rule.t()) :: boolean()
  def rule_child_started?(%Rule{backend_id: backend_id, source_id: source_id}) do
    via = Backends.via_source(source_id, AdaptorSupervisor, backend_id)

    if GenServer.whereis(via) do
      true
    else
      false
    end
  end

  @doc """
  Starts a given backend child spec for the backend associated with a rule.
  This backend will not be registered for ingest dispatching.

  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.
  """
  @spec start_rule_child(Rule.t()) :: Supervisor.on_start_child()
  def start_rule_child(%Rule{backend_id: backend_id} = rule) do
    backend = Backends.Cache.get_backend(backend_id) |> Map.put(:register_for_ingest, false)
    source = Sources.Cache.get_by_id(rule.source_id)
    start_backend_child(source, backend)
  end

  @doc """
  Starts a given backend-souce combination when SourceSup is already running.
  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.
  """
  @spec start_backend_child(Source.t(), Backend.t()) :: Supervisor.on_start_child()
  def start_backend_child(%Source{} = source, %Backend{} = backend) do
    via = Backends.via_source(source, __MODULE__)
    source = Sources.Cache.get_by_id(source.id)
    spec = Backend.child_spec(source, backend)
    Supervisor.start_child(via, spec)
  end

  @doc """
  Stops a given backend child on SourceSup that is associated with the given Rule.
  """
  @spec stop_rule_child(Rule.t()) :: :ok | {:error, :not_found}
  def stop_rule_child(%Rule{backend_id: backend_id} = rule) do
    backend = Backends.Cache.get_backend(backend_id) |> Map.put(:register_for_ingest, false)
    source = Sources.Cache.get_by_id(rule.source_id)
    stop_backend_child(source, backend)
  end

  @doc """
  Stops a backend child based on a provide source-backend combination.
  """
  @spec stop_backend_child(Source.t(), Backend.t()) :: :ok | {:error, :not_found}
  def stop_backend_child(%Source{} = source, %Backend{id: id}), do: stop_backend_child(source, id)

  def stop_backend_child(%Source{} = source, backend_id) when backend_id != nil do
    via = Backends.via_source(source, __MODULE__)
    # spec = Backend.child_spec(source, backend)

    found_id =
      Supervisor.which_children(via)
      |> Enum.find_value(
        fn {{_mod, _source_id, bid}, _pid, _type, _sup} ->
          bid == backend_id
        end,
        &elem(&1, 0)
      )

    if found_id do
      Supervisor.terminate_child(via, found_id)
      Supervisor.delete_child(via, found_id)
      :ok
    else
      {:error, :not_found}
    end
  end
end
