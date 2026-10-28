defmodule Logflare.Rules do
  @moduledoc false
  import Ecto.Query

  alias Logflare.Lql
  alias Logflare.Repo
  alias Logflare.Rules.Rule
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.RulesTree
  alias Logflare.SourceSchemas
  alias Logflare.Backends.Backend
  alias Logflare.Backends.SourceSup
  alias Logflare.Cluster
  alias Logflare.ContextCache

  @doc """
  Lists rules for a given Source or Backend
  """
  @spec list_rules(Source.t() | Backend.t()) :: [Rule.t()]
  def list_rules(%Source{id: source_id}), do: list_by_source_id(source_id)
  def list_rules(%Backend{id: backend_id}), do: list_by_backend_id(backend_id)

  # Cache friendly getters
  def list_by_source_id(source_id) do
    from(r in Rule, where: r.source_id == ^source_id)
    |> Repo.all()
  end

  def list_by_backend_id(backend_id) do
    from(r in Rule, where: r.backend_id == ^backend_id)
    |> Repo.all()
  end

  @doc "Lists rules owned by a user through their source."
  @spec list_rules_by_user_id(pos_integer(), non_neg_integer() | nil) :: [Rule.t()]
  def list_rules_by_user_id(user_id, backend_id \\ nil) do
    from(r in Rule,
      join: s in Source,
      on: r.source_id == s.id,
      where: s.user_id == ^user_id
    )
    |> maybe_filter_by_backend_id(backend_id)
    |> Repo.all()
  end

  defp maybe_filter_by_backend_id(query, nil), do: query

  defp maybe_filter_by_backend_id(query, backend_id) do
    where(query, [r], r.backend_id == ^backend_id)
  end

  @spec rules_tree_by_source_id(pos_integer()) :: RulesTree.t()
  def rules_tree_by_source_id(id) do
    from(r in Rule,
      left_join: b in Backend,
      on: r.backend_id == b.id,
      where: r.source_id == ^id and (is_nil(r.backend_id) or b.enabled)
    )
    |> Repo.all()
    |> RulesTree.build()
  end

  @doc """
  Lists rules that are related to some backend.
  """
  def list_rules_with_backend(%Source{id: source_id}) do
    from(r in Rule, where: r.source_id == ^source_id, where: not is_nil(r.backend_id))
    |> Repo.all()
  end

  @doc """
  Creates a rule based on a given attr map.
  If it is a drain rule with an associated backend, it will attempt to start the backend child on SourceSup if it is running.
  """
  @spec create_rule(map()) :: {:ok, Rule.t()} | {:error, Ecto.Changeset.t()}
  def create_rule(attrs \\ %{}) do
    %Rule{}
    |> Rule.changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, %Rule{backend_id: backend_id} = rule} = result when backend_id != nil ->
        # expected to fail on nodes where SourceSup is not started
        Cluster.Utils.rpc_multicall(SourceSup, :start_rule_child, [rule])

        result

      other ->
        other
    end
  end

  @doc """
  Updates a given rule.
  """
  @spec update_rule(Rule.t(), map()) :: {:ok, Rule.t()} | {:error, Ecto.Changeset.t()}
  def update_rule(%Rule{} = rule, attrs) do
    rule
    |> Rule.changeset(attrs)
    |> Repo.update()
  end

  @doc "Retrieves a given Rule by id. Returns nil if not present."
  @spec get_rule(non_neg_integer()) :: Rule.t() | nil
  def get_rule(id), do: Repo.get(Rule, id)

  def fetch_rule_by(kw) do
    {user_id, kw} = Keyword.pop(kw, :user_id)

    from(r in Rule, join: s in Source, on: s.user_id == ^user_id)
    |> where([r], ^kw)
    |> Repo.one()
    |> case do
      nil -> {:error, :not_found}
      rule -> {:ok, rule}
    end
  end

  @spec create_rule(map(), Source.t()) :: {:ok, Rule.t()} | {:error, Ecto.Changeset.t() | binary}
  def create_rule(params, %Source{} = source) when is_map(params) do
    bq_schema =
      SourceSchemas.get_source_schema_by(source_id: source.id) |> Map.get(:bigquery_schema)

    lql_string = params["lql_string"]

    with {:ok, lql_filters} <- Lql.Parser.parse(lql_string, bq_schema),
         params = Map.put(params, "lql_filters", lql_filters),
         {:ok, rule} <- Rule.changeset(%Rule{source_id: source.id}, params) |> Repo.insert() do
      {:ok, rule}
    else
      {:error, %Ecto.Changeset{} = changeset} -> {:error, changeset}
      errtup -> errtup
    end
  end

  @doc """
  Deletes a given rule.
  Atempts to stop the rules associated backend child if SourceSup is started.
  """
  @spec delete_rule(Rule.t()) :: {:ok, Rule.t()}
  def delete_rule(rule) do
    res = Repo.delete(rule)
    Cluster.Utils.rpc_multicall(SourceSup, :stop_rule_child, [rule])

    res
  end

  @doc """
  Ensures that the rule's backend is started.
  If the rule does not exist, it is a noop.
  """
  @spec sync_rule(integer()) :: :ok
  def sync_rule(rule_id) do
    if rule = get_rule(rule_id) do
      # ensure that rules backends are started

      if SourceSup.rule_child_started?(rule) == false do
        SourceSup.start_rule_child(rule)
      end
    end

    :ok
  end

  @doc """
  Syncs rules across all cluster nodes for sources with drain rules targeting this backend.

  This invalidates cached rule lists and routing trees when the backend changes,
  including trees cached as empty while the backend was disabled.
  """
  @spec sync_rules_across_cluster(pos_integer()) :: :ok
  def sync_rules_across_cluster(backend_id) do
    # Query once on the sending node so receiving nodes only evict cached entries.
    source_ids =
      for rule <- list_by_backend_id(backend_id), uniq: true do
        rule.source_id
      end

    if source_ids != [] do
      Cluster.Utils.rpc_multicast(__MODULE__, :sync_rules_local, [source_ids])
    end

    :ok
  end

  @doc false
  @spec sync_rules_local([pos_integer()]) :: :ok
  def sync_rules_local(source_ids) do
    source_ids
    |> Enum.map(&{__MODULE__, source_id: &1})
    |> ContextCache.bust_keys()

    :ok
  end
end
