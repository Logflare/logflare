defmodule Logflare.Backends.SourceSup do
  @moduledoc false
  use Supervisor

  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSupWorker
  alias Logflare.Backends
  alias Logflare.Sources.Source
  alias Logflare.Users
  alias Logflare.Billing
  alias Logflare.Sources.Source.RateCounterServer
  alias Logflare.Sources.Source.EmailNotificationServer
  alias Logflare.Sources.Source.TextNotificationServer
  alias Logflare.Sources.Source.WebhookNotificationServer
  alias Logflare.Sources.Source.SlackHookServer
  alias Logflare.Sources.Source.BillingWriter
  alias Logflare.Backends.RecentInsertsCacher
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Backends.AdaptorSupervisor

  @doc """
  Returns the child spec for the supervision tree of a source.

  The start arguments hold only the source id. A source with many rules is about 1 MB. The
  `SourcesSup` partition copies the start arguments into each `start_child` message. The
  partition also keeps them in its state. `init/1` loads the source and its rules from the cache.
  """
  @spec child_spec(Source.t() | pos_integer()) :: Supervisor.child_spec()
  def child_spec(%Source{id: id}), do: child_spec(id)

  def child_spec(source_id) when is_integer(source_id) do
    %{
      id: {__MODULE__, source_id},
      start: {__MODULE__, :start_link, [source_id]},
      restart: :transient
    }
  end

  @spec start_link(pos_integer()) :: Supervisor.on_start()
  def start_link(source_id) when is_integer(source_id) do
    Supervisor.start_link(__MODULE__, source_id, name: Backends.via_source(source_id, __MODULE__))
  end

  @doc """
  Warms cache-backed reads performed while a SourceSup and its initial children start.

  `DynamicSupervisor.start_child/2` runs the child initialization in the new SourceSup process,
  but the `SourcesSup` partition waits synchronously for the initial supervision tree to start.
  On a cold cache, a Postgres fallback therefore prevents that partition from starting any other
  source until the lookup returns.

  Calling this first moves those misses to the caller. `init/1` remains unchanged so automatic
  crash restarts still resolve configuration through the caches and pick up changes.

  Cache warmers do not make this redundant: they are capped, run asynchronously, and are
  registered `required: false`.
  """
  @spec prefetch(Source.t()) :: :ok
  def prefetch(%Source{} = source) do
    Sources.Cache.get_by_id(source.id)
    Rules.Cache.rules_tree_by_source_id(source.id)

    source_backends =
      Backends.Cache.list_backends(source_id: source.id)
      |> Enum.reject(& &1.consolidated_ingest?)

    rules_backends =
      Backends.Cache.list_backends(rules_source_id: source.id)
      |> Enum.reject(& &1.consolidated_ingest?)

    user = Users.Cache.get(source.user_id)
    Billing.Cache.get_plan_by_user(user)

    started_backends =
      [Backends.get_default_backend(user) | source_backends]
      |> Enum.concat(rules_backends)

    if Enum.any?(started_backends, &(&1.type == :bigquery)) do
      SourceSchemas.Cache.get_source_schema_by(source_id: source.id)
    end

    :ok
  end

  @doc """
  Loads the source and starts its children. Returns `:ignore` when the source does not exist.

  The cache can hold `nil` for a source that exists. The children read the source from the same
  cache entry. Thus `init/1` uses `Sources.Cache.get_by_id_or_primary/1`, which reads the primary
  database on a cached `nil` and repairs the entry.
  """
  def init(source_id) do
    case Sources.Cache.get_by_id_or_primary(source_id) do
      nil -> :ignore
      source -> init_children(source)
    end
  end

  defp init_children(source) do
    ingest_backends =
      Backends.Cache.list_backends(source_id: source.id)
      |> Enum.reject(& &1.consolidated_ingest?)

    rules_backends =
      Backends.Cache.list_backends(rules_source_id: source.id)
      |> Enum.reject(& &1.consolidated_ingest?)
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
        {RecentInsertsCacher, [source: source]},
        {EmailNotificationServer, [source: source]},
        {TextNotificationServer, [source: source, plan: plan]},
        {WebhookNotificationServer, [source: source]},
        {SlackHookServer, [source: source]},
        {BillingWriter, [source: source]}
      ] ++
        if(Application.get_env(:logflare, :env) != :test,
          do: [{SourceSupWorker, [source: source]}],
          else: []
        ) ++ specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Checks if a rule child is started for a given source/rule.
  Must be a backend rule.
  """
  @spec rule_child_started?(Rule.t()) :: boolean()
  def rule_child_started?(%Rule{backend_id: backend_id, source_id: source_id})
      when backend_id != nil,
      do: backend_child_started?(backend_id, source_id)

  @doc """
  Checks if a given backend associated with source is started.
  """
  @spec backend_child_started?(non_neg_integer(), non_neg_integer()) :: boolean()
  def backend_child_started?(backend_id, source_id) do
    via = Backends.via_source(source_id, AdaptorSupervisor, backend_id)

    if GenServer.whereis(via) do
      true
    else
      false
    end
  end

  @doc """
  Wrapper calling `start_backend_child_by_id/2` with backend and source ids
  associated with given rule
  """
  @spec start_rule_child(Rule.t()) :: Supervisor.on_start_child() | :noop
  def start_rule_child(%Rule{} = rule),
    do: start_backend_child_by_id(rule.backend_id, rule.source_id)

  @doc """
  Starts a backend child spec for the given backend id and source id.
  This backend will not be registered for ingest dispatching.

  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.
  """
  @spec start_backend_child_by_id(non_neg_integer(), non_neg_integer()) ::
          Supervisor.on_start_child() | :noop
  def start_backend_child_by_id(backend_id, source_id) do
    backend = Backends.Cache.get_backend(backend_id) |> Map.put(:register_for_ingest, false)
    source = Sources.Cache.get_by_id(source_id)
    start_backend_child(source, backend)
  end

  @doc """
  Starts a given backend-souce combination when SourceSup is already running.
  This allows for zero-downtime ingestion, as we don't restart the SourceSup supervision tree.

  Consolidated backends are excluded.
  """
  @spec start_backend_child(Source.t(), Backend.t()) :: Supervisor.on_start_child() | :noop
  def start_backend_child(%Source{}, %Backend{consolidated_ingest?: true}), do: :noop

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
      |> Enum.find_value(fn
        {{_mod, _source_id, ^backend_id} = child_id, _pid, _type, _sup} -> child_id
        _child -> nil
      end)

    if found_id do
      Supervisor.terminate_child(via, found_id)
      Supervisor.delete_child(via, found_id)
      :ok
    else
      {:error, :not_found}
    end
  end
end
