defmodule Logflare.Endpoints do
  @moduledoc false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.Endpoints.PiiRedactor
  alias Logflare.Endpoints.Query
  alias Logflare.Endpoints.Resolver
  alias Logflare.Endpoints.ResultsCache
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.Teams
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Utils
  alias PaperTrail.Version

  @valid_sql_languages ~w(bq_sql ch_sql pg_sql)a
  @endpoint_version_snapshot_fields [
    :token,
    :name,
    :query,
    :description,
    :language,
    :source_mapping,
    :sandboxable,
    :cache_duration_seconds,
    :proactive_requerying_seconds,
    :max_limit,
    :enable_auth,
    :redact_pii,
    :enable_dynamic_reservation,
    :labels,
    :backend_id
  ]

  @typep language :: :bq_sql | :ch_sql | :pg_sql | :lql
  @typep originator :: User.t() | TeamUser.t() | OauthAccessToken.t()
  @typep run_query_return ::
           {:ok, %{required(:rows) => [term()], optional(atom()) => any()}}
           | {:error, String.t()}

  defguardp is_integer_or_string(value) when is_integer(value) or is_non_empty_binary(value)

  @spec count_endpoints_by_user(User.t()) :: integer()
  def count_endpoints_by_user(%User{id: user_id}), do: count_endpoints_by_user(user_id)

  @spec count_endpoints_by_user(user_id :: integer()) :: integer()
  def count_endpoints_by_user(user_id) when is_integer(user_id) do
    q = from(s in Query, where: s.user_id == ^user_id)
    Repo.aggregate(q, :count)
  end

  @spec list_endpoints_by(Keyword.t()) :: [Query.t()] | []
  def list_endpoints_by(kw) when is_list(kw) do
    q = from(e in Query)

    Enum.reduce(kw, q, fn {k, v}, q ->
      case k do
        :name -> where(q, [e], e.name == ^v)
        :id -> where(q, [e], e.id == ^v)
        :user_id -> where(q, [e], e.user_id == ^v)
      end
    end)
    |> Repo.all()
  end

  @doc """
  Lists all endpoints a user has access to, including where the user is a team member.
  """
  @spec list_endpoints_by_user_access(User.t()) :: [Query.t()]
  def list_endpoints_by_user_access(%User{} = user) do
    Query
    |> Teams.filter_by_user_access(user)
    |> Repo.all()
  end

  @doc """
  Gets an endpoint query by id that the user has access to.
  Returns the endpoint query if the user owns it or is a team member, otherwise returns nil.
  """
  @spec get_endpoint_query_by_user_access(User.t() | TeamUser.t(), integer() | String.t()) ::
          Query.t() | nil
  def get_endpoint_query_by_user_access(user_or_team_user, id)
      when is_integer(id) or is_binary(id) do
    Query
    |> Teams.filter_by_user_access(user_or_team_user)
    |> where([query], query.id == ^id)
    |> Repo.one()
  end

  @doc """
  Retrieves an endpoint `Query`
  """
  @spec get_endpoint_query(query_id :: integer() | String.t()) :: Query.t() | nil
  def get_endpoint_query(query_id) when is_integer_or_string(query_id),
    do: Repo.get(Query, query_id)

  @doc """
  Retrieves a mapped endpoint `Query` by token
  """
  @spec get_mapped_query_by_token(token :: String.t()) :: Query.t() | nil
  def get_mapped_query_by_token(token) when is_non_empty_binary(token) do
    get_by(token: token)
    |> Query.map_query_sources()
    |> Repo.preload(:user)
  end

  @doc """
  Puts the `:query` key of the `Query` with the latest source mappings.
  This ensure that the query will have the latest source names (assuming a name change)
  """
  @spec map_query_sources(Query.t() | nil) :: Query.t() | nil
  def map_query_sources(nil), do: nil
  def map_query_sources(%Query{} = query), do: Query.map_query_sources(query)

  @spec get_by(Keyword.t()) :: Query.t() | nil
  def get_by(kw), do: Repo.get_by(Query, kw)

  @spec create_query(User.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def create_query(%User{} = user, params) when is_map(params) do
    create_query(user, params, user)
  end

  @spec create_query(User.t(), map(), originator()) :: {:ok, Query.t()} | {:error, any()}
  def create_query(%User{} = user, params, originator) when is_map(params) do
    user
    |> Ecto.build_assoc(:endpoint_queries)
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> then(fn changeset ->
      PaperTrail.insert(changeset, paper_trail_opts(changeset, originator, 1))
    end)
    |> case do
      {:ok, %{model: endpoint}} -> {:ok, endpoint}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "returns an ecto changeset for changing an endpoint."
  @spec change_query(Query.t(), map()) :: Ecto.Changeset.t()
  def change_query(%Query{} = query, attrs \\ %{}) do
    query
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(attrs)
  end

  @doc """
  Derives the SQL language from a backend ID.

  Returns `:bq_sql` if no backend ID is specified or if the backend ID is invalid.
  """
  @spec derive_language_from_backend_id(String.t() | integer() | nil) :: atom()
  def derive_language_from_backend_id(backend_id) when is_non_empty_binary(backend_id) do
    case Integer.parse(backend_id) do
      {id, ""} -> derive_language_from_backend_id(id)
      _ -> :bq_sql
    end
  end

  def derive_language_from_backend_id(backend_id) when is_integer(backend_id) do
    case Backends.get_backend(backend_id) do
      %Backend{} = backend ->
        Query.map_backend_to_language(backend, SingleTenant.supabase_mode?())

      nil ->
        :bq_sql
    end
  end

  def derive_language_from_backend_id(_), do: :bq_sql

  @spec update_query(Query.t(), map(), originator()) :: {:ok, Query.t()} | {:error, any()}
  def update_query(%Query{} = query, params, originator) when is_map(params) do
    endpoint = Repo.preload(query, :user)
    changeset = Query.update_by_user_changeset(endpoint, params)

    Repo.transaction(fn ->
      endpoint = lock_endpoint_query(query)
      version_number = next_endpoint_version_number(endpoint.id)
      changeset = %{changeset | data: endpoint}

      case PaperTrail.update(
             changeset,
             paper_trail_opts(changeset, originator, version_number)
           ) do
        {:ok, %{model: endpoint}} ->
          {endpoint, should_kill_caches?(changeset.changes)}

        {:error, changeset} ->
          Repo.rollback(changeset)
      end
    end)
    |> case do
      {:ok, {endpoint, should_kill_caches?}} ->
        maybe_kill_endpoint_caches(endpoint, should_kill_caches?)
        {:ok, endpoint}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec paper_trail_opts(Ecto.Changeset.t(), originator(), integer() | nil) :: keyword()
  defp paper_trail_opts(changeset, originator, version_number) do
    [origin: version_origin(originator), meta: version_meta(changeset, version_number)]
  end

  @spec version_meta(Ecto.Changeset.t(), integer() | nil) :: map()
  defp version_meta(changeset, version_number) do
    %{
      endpoint_snapshot: endpoint_version_snapshot(changeset)
    }
    |> maybe_put_query_diff(changeset)
    |> maybe_put_version_number(version_number)
  end

  @spec endpoint_version_snapshot(Ecto.Changeset.t()) :: map()
  defp endpoint_version_snapshot(changeset) do
    changeset
    |> Ecto.Changeset.apply_changes()
    |> Map.take(@endpoint_version_snapshot_fields)
  end

  @spec maybe_put_version_number(map(), integer()) :: map()
  defp maybe_put_version_number(meta, version_number),
    do: Map.put(meta, :version_number, version_number)

  @spec maybe_put_query_diff(map(), Ecto.Changeset.t()) :: map()
  defp maybe_put_query_diff(meta, changeset) do
    case endpoint_version_query_diff(changeset) do
      [] -> meta
      query_diff -> Map.put(meta, :query_diff, query_diff)
    end
  end

  @spec endpoint_version_query_diff(Ecto.Changeset.t()) ::
          [%{required(:type) => String.t(), required(:value) => String.t()}]
  defp endpoint_version_query_diff(changeset) do
    previous_query = changeset.data.query
    current_query = Ecto.Changeset.get_field(changeset, :query)

    if previous_query != current_query do
      previous_query
      |> to_query_words()
      |> List.myers_difference(to_query_words(current_query))
      |> Enum.map(fn {type, words} ->
        value = Enum.join(words, " ")

        %{
          type: Atom.to_string(type),
          value: if(value == "", do: "", else: value <> " ")
        }
      end)
      |> Enum.reject(&(&1.value == ""))
    else
      []
    end
  end

  @spec to_query_words(String.t() | nil) :: [String.t()]
  defp to_query_words(query) when is_binary(query), do: String.split(query)
  defp to_query_words(nil), do: []

  @spec version_origin(originator()) :: String.t() | nil
  defp version_origin(%User{email: email}) when is_non_empty_binary(email), do: email
  defp version_origin(%TeamUser{email: email}) when is_non_empty_binary(email), do: email

  defp version_origin(%OauthAccessToken{description: description})
       when is_non_empty_binary(description) do
    "API: #{description}"
  end

  defp version_origin(%OauthAccessToken{}), do: "API: unknown"

  @spec lock_endpoint_query(Query.t()) :: Query.t()
  defp lock_endpoint_query(%Query{id: query_id}) do
    from(query in Query,
      where: query.id == ^query_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one!()
    |> Repo.preload(:user)
  end

  @spec next_endpoint_version_number(integer()) :: integer()
  defp next_endpoint_version_number(endpoint_id) do
    from(version in Version,
      where: version.item_type == "Query" and version.item_id == ^endpoint_id,
      select: fragment("COALESCE(MAX((?->>'version_number')::integer), 0)", version.meta)
    )
    |> Repo.one()
    |> Kernel.+(1)
  end

  @spec should_kill_caches?(map()) :: boolean()
  defp should_kill_caches?(changes) when is_map(changes) do
    Enum.any?(Map.keys(changes), fn key ->
      key in [
        :query,
        :sandboxable,
        :cache_duration_seconds,
        :proactive_requerying_seconds,
        :max_limit,
        :enable_auth,
        :labels
      ]
    end)
  end

  @spec maybe_kill_endpoint_caches(Query.t(), boolean()) :: :ok | [term()]
  defp maybe_kill_endpoint_caches(_endpoint, false), do: :ok

  defp maybe_kill_endpoint_caches(endpoint, true) do
    for pid <- Resolver.list_caches(endpoint) do
      Utils.Tasks.async(fn ->
        ResultsCache.invalidate(pid)
      end)
    end
    |> Task.await_many(30_000)
  end

  @spec get_endpoint_query_version_by_version_number(
          integer() | String.t(),
          integer() | String.t()
        ) ::
          Version.t() | nil
  def get_endpoint_query_version_by_version_number(endpoint_id, version_number)
      when is_integer_or_string(endpoint_id) and is_integer_or_string(version_number) do
    version_number = to_string(version_number)

    from(version in Version,
      where:
        version.item_type == "Query" and version.item_id == ^endpoint_id and
          fragment("?->>'version_number' = ?", version.meta, ^version_number)
    )
    |> Repo.one()
  end

  @spec delete_query(Query.t(), originator()) :: {:ok, Query.t()} | {:error, any()}
  def delete_query(%Query{} = query, originator) do
    Repo.transaction(fn ->
      endpoint = lock_endpoint_query(query)
      version_number = next_endpoint_version_number(endpoint.id)
      changeset = Ecto.Changeset.change(endpoint)
      opts = paper_trail_opts(changeset, originator, version_number)

      case PaperTrail.delete(endpoint, opts) do
        {:ok, %{model: deleted_endpoint}} -> deleted_endpoint
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  @doc """
  Parses a query string without running it.
  """
  @spec parse_query_string(
          language :: :bq_sql | :ch_sql | :pg_sql,
          query_string :: String.t(),
          endpoint_queries :: [Query.t()],
          alerts :: [AlertQuery.t()]
        ) ::
          {:ok, %{parameters: [String.t()], expanded_query: String.t()}} | {:error, any()}
  def parse_query_string(language, query_string, endpoint_queries, alerts)
      when language in @valid_sql_languages and is_non_empty_binary(query_string) and
             is_list(endpoint_queries) and is_list(alerts) do
    with {:ok, expanded_query} <-
           Sql.expand_subqueries(
             language,
             query_string,
             endpoint_queries ++ alerts
           ),
         {:ok, declared_params} <- Sql.parameters(expanded_query) do
      {:ok, %{parameters: declared_params, expanded_query: expanded_query}}
    end
  end

  @doc """
  Parses endpoint labels from allowlist configuration, request headers, and query parameters.

  This function processes label configurations that can come from three sources:
  1. Static values defined in the allowlist string
  2. Dynamic values from query parameters (prefixed with "@")
  3. Values from request headers

  The allowlist string defines which labels are allowed and how they should be populated.
  It supports three formats:
  - `key=value` - static label with fixed value
  - `key=@param` - dynamic label populated from query parameter "param"
  - `key` - label populated from request header with the same key name

  ## Parameters

    * `allowlist_str` - Comma-separated string defining allowed labels and their sources
    * `header_str` - Comma-separated string from LF-ENDPOINT-LABELS header containing key=value pairs
    * `params` - Map of query parameters that may contain label values

  ## Examples

      # Static labels
      iex> Logflare.Endpoints.parse_labels("environment=production,team=backend", "", %{})
      %{"environment" => "production", "team" => "backend"}

      # Header-based labels
      iex> Logflare.Endpoints.parse_labels("user_id,session_id", "user_id=123,session_id=abc", %{})
      %{"user_id" => "123", "session_id" => "abc"}

      # Parameter-based labels
      iex> Logflare.Endpoints.parse_labels("tenant=@tenant_id", "", %{"tenant_id" => "org-123"})
      %{"tenant" => "org-123"}

      # Mixed sources with fallback
      iex> Logflare.Endpoints.parse_labels("user=@user_id", "user=999", %{"user_id" => "123"})
      %{"user" => "123"}

      # Fallback to header when param missing
      iex> Logflare.Endpoints.parse_labels("user=@user_id", "user=999", %{})
      %{"user" => "999"}

      # Empty or nil inputs
      iex> Logflare.Endpoints.parse_labels(nil, nil, %{})
      %{}

      iex> Logflare.Endpoints.parse_labels("", "", %{})
      %{}

  ## Returns

  A map where keys are label names and values are the resolved label values.
  """
  @spec parse_labels(String.t() | nil, String.t() | nil, map()) :: map()
  def parse_labels(allowlist_str, header_str, params) do
    header_values =
      for item <- String.split(header_str || "", ","), into: %{} do
        case String.split(item, "=") do
          [key, value] -> {key, value}
          [key] -> {key, nil}
        end
      end

    for split <- String.split(allowlist_str || "", ","), split != "", into: %{} do
      case String.split(split, "=") do
        [key, "@" <> param_key] ->
          {key, Map.get(params, param_key) || Map.get(header_values, key)}

        [key] ->
          {key, Map.get(header_values, key)}

        [key, value] ->
          {key, value}
      end
    end
  end

  @doc """
  Runs a an endpoint query
  """
  @spec run_query(Query.t(), params :: map(), opts :: Keyword.t()) :: run_query_return()
  def run_query(%Query{language: query_language} = endpoint_query, params \\ %{}, opts \\ [])
      when is_map(params) and is_list(opts) do
    %Query{query: query_string, user_id: user_id, sandboxable: sandboxable} = endpoint_query
    sql_param = Map.get(params, "sql")
    lql_param = Map.get(params, "lql")

    endpoints =
      list_endpoints_by(user_id: endpoint_query.user_id)
      |> Enum.filter(&(&1.id != endpoint_query.id))

    alerts = Alerting.list_alert_queries_by_user_id(endpoint_query.user_id)

    with {:ok, declared_params} <- Sql.parameters(query_string),
         {:ok, expanded_query} <-
           Sql.expand_subqueries(
             query_language,
             query_string,
             endpoints ++ alerts
           ),
         {:ok, consumer_query} <-
           maybe_convert_lql_to_sql(
             lql_param,
             sql_param,
             expanded_query,
             query_language,
             sandboxable
           ),
         transform_input =
           if(sandboxable && consumer_query,
             do: {expanded_query, consumer_query},
             else: expanded_query
           ),
         {:ok, transformed_query} <-
           Sql.transform(query_language, transform_input, user_id) do
      :telemetry.span(
        [:logflare, :endpoints, :run_query, :exec_query_on_backend],
        %{endpoint_id: endpoint_query.id, language: query_language},
        fn ->
          exec_query_on_backend(
            endpoint_query,
            transformed_query,
            declared_params,
            params,
            opts
          )
          |> then(fn
            {:ok, data} = result ->
              measurements = %{
                total_bytes_processed: Map.get(data, :total_bytes_processed, 0)
              }

              metadata =
                Map.merge(endpoint_query.parsed_labels || %{}, %{
                  "endpoint_id" => endpoint_query.id,
                  "endpoint_uuid" => Utils.stringify(endpoint_query.token),
                  "user_id" => endpoint_query.user_id
                })

              :telemetry.execute([:logflare, :endpoints, :query], measurements, metadata)

              {result, %{}}

            result ->
              {result, %{}}
          end)
        end
      )
    end
  end

  @doc """
  Runs a query string

  ### Example
    iex> run_query_string(%User{...}, {:bq_sql, "select current_time() where @value > 4"}, params: %{"value" => "123"})
    {:ok, %{rows:  [...]} }
  """
  @spec run_query_string(User.t(), {language(), String.t()}, opts :: Keyword.t()) ::
          run_query_return()
  def run_query_string(user, {language, query_string}, opts \\ []) do
    params = Keyword.get(opts, :params, %{})

    source_mapping =
      user
      |> Users.preload_sources()
      |> then(fn %{sources: sources} -> sources end)
      |> Enum.map(&{&1.name, &1.token})

    query = %Query{
      query: query_string,
      language: language,
      sandboxable: Keyword.get(opts, :sandboxable, false),
      user: user,
      user_id: user.id,
      parsed_labels: Keyword.get(opts, :parsed_labels, %{}),
      source_mapping: source_mapping,
      redact_pii: Keyword.get(opts, :redact_pii, false),
      backend_id: Keyword.get(opts, :backend_id)
    }

    run_query(query, params, opts)
  end

  @doc """
  Runs a cached query.
  """
  @spec run_cached_query(query :: Query.t(), params :: map()) :: run_query_return()
  def run_cached_query(%Query{} = query, params \\ %{}, opts \\ [])
      when is_map(params) and is_list(opts) do
    if query.cache_duration_seconds > 0 do
      query
      |> Resolver.resolve(params, opts)
      |> ResultsCache.query()
    else
      # execute the query directly
      run_query(query, params, opts)
    end
  end

  @doc """
  Calculates and sets the `:metrics` key with `Query.Metrics`, which contains info and stats relating to the endpoint
  """
  @spec calculate_endpoint_metrics(Query.t() | [Query.t()]) :: Query.t() | [Query.t()]
  def calculate_endpoint_metrics(endpoint_queries) when is_list(endpoint_queries) do
    for endpoint_query <- endpoint_queries, do: calculate_endpoint_metrics(endpoint_query)
  end

  def calculate_endpoint_metrics(%Query{} = endpoint_query) do
    cache_count = endpoint_query |> Resolver.list_caches() |> length()

    %{
      endpoint_query
      | metrics: %Query.Metrics{
          cache_count: cache_count
        }
    }
  end

  @doc """
  Returns the transformed query without executing it.

  Useful for debugging and UI display of merged sandbox queries.
  """
  @spec get_transformed_query(Query.t(), params :: map()) ::
          {:ok, String.t()} | {:error, String.t()}
  def get_transformed_query(%Query{} = endpoint_query, params \\ %{}) when is_map(params) do
    %Query{
      query: query_string,
      user_id: user_id,
      sandboxable: sandboxable,
      language: query_language
    } =
      endpoint_query

    sql_param = Map.get(params, "sql")
    lql_param = Map.get(params, "lql")

    endpoints =
      list_endpoints_by(user_id: endpoint_query.user_id)
      |> Enum.filter(&(&1.id != endpoint_query.id))

    alerts = Alerting.list_alert_queries_by_user_id(endpoint_query.user_id)

    with {:ok, expanded_query} <-
           Sql.expand_subqueries(
             query_language,
             query_string,
             endpoints ++ alerts
           ),
         {:ok, consumer_query} <-
           maybe_convert_lql_to_sql(
             lql_param,
             sql_param,
             expanded_query,
             query_language,
             sandboxable
           ),
         transform_input =
           if(sandboxable && consumer_query,
             do: {expanded_query, consumer_query},
             else: expanded_query
           ),
         {:ok, transformed_query} <- Sql.transform(query_language, transform_input, user_id) do
      {:ok, transformed_query}
    end
  end

  @spec maybe_convert_lql_to_sql(
          lql_param :: String.t() | nil,
          sql_param :: String.t() | nil,
          expanded_query :: String.t(),
          language :: :bq_sql | :ch_sql | :pg_sql,
          sandboxable :: boolean()
        ) :: {:ok, String.t() | nil} | {:error, String.t()}
  # no sql_param provided, but lql_param is present for SANDBOXED endpoint
  defp maybe_convert_lql_to_sql(lql_param, nil, expanded_query, language, true)
       when is_non_empty_binary(lql_param) and language in [:bq_sql, :ch_sql, :pg_sql] do
    with {:ok, cte_names} <- Sql.extract_cte_aliases(expanded_query),
         {:ok, lql_rules} <- Lql.Parser.parse(lql_param),
         from_rule <- Rules.get_from_rule(lql_rules),
         {:ok, cte_table_name} <- validate_from_rule_for_sandbox(from_rule, cte_names),
         dialect <- Lql.language_to_dialect(language),
         {:ok, sql_string} <- Lql.to_sandboxed_sql(lql_param, cte_table_name, dialect) do
      {:ok, sql_string}
    end
  end

  # If sql_param is provided, use it (takes precedence over lql_param)
  defp maybe_convert_lql_to_sql(_lql_param, sql_param, _expanded_query, _language, _sandboxable)
       when is_non_empty_binary(sql_param) do
    {:ok, sql_param}
  end

  # No lql or sql param, return nil
  defp maybe_convert_lql_to_sql(_lql_param, _sql_param, _expanded_query, _language, _sandboxable) do
    {:ok, nil}
  end

  # Multiple CTEs, no FromRule → ERROR (user must specify which CTE)
  defp validate_from_rule_for_sandbox(nil, [_, _ | _] = cte_names) do
    available = Enum.join(cte_names, ", ")

    {:error,
     "Multiple CTEs available (#{available}). You must specify which one to query using `f:name`"}
  end

  # Single CTE, no FromRule → Use that CTE
  defp validate_from_rule_for_sandbox(nil, [cte_name]) do
    {:ok, cte_name}
  end

  # No CTEs at all (edge case)
  defp validate_from_rule_for_sandbox(nil, []) do
    {:error, "No CTEs found in query"}
  end

  # FromRule specified, validate it exists in CTEs
  defp validate_from_rule_for_sandbox(%FromRule{table: table}, cte_names)
       when is_list(cte_names) do
    if Enum.member?(cte_names, table) do
      {:ok, table}
    else
      available = Enum.join(cte_names, ", ")
      {:error, "Table '#{table}' not found in available CTEs: #{available}"}
    end
  end

  @spec exec_query_on_backend(
          endpoint_query :: Query.t(),
          transformed_query :: String.t(),
          declared_params :: [String.t()],
          input_params :: map(),
          opts :: Keyword.t()
        ) :: run_query_return()
  defp exec_query_on_backend(
         %Query{language: sql_language} = endpoint_query,
         transformed_query,
         declared_params,
         input_params,
         opts
       )
       when sql_language in @valid_sql_languages and is_binary(transformed_query) and
              is_list(declared_params) and is_map(input_params) and is_list(opts) do
    with {:ok, %Backend{} = backend} <- get_backend_for_query(endpoint_query),
         adaptor <- Backends.Adaptor.get_adaptor(backend) do
      # let the adaptor transform the query if needed
      final_query =
        if Backends.Adaptor.can_transform_query?(backend) do
          transformation_context = build_transformation_context(backend)

          case adaptor.transform_query(transformed_query, sql_language, transformation_context) do
            {:ok, adapted_query} -> adapted_query
            # fallback to original if transformation fails
            {:error, _} -> transformed_query
          end
        else
          transformed_query
        end

      # handle parameter mapping
      query_args =
        if Backends.Adaptor.can_map_query_parameters?(backend) do
          mapped_params =
            adaptor.map_query_parameters(
              endpoint_query.query,
              final_query,
              declared_params,
              input_params
            )

          {final_query, mapped_params}
        else
          {final_query, declared_params, input_params, endpoint_query}
        end

      redact_pii = Keyword.get(opts, :redact_pii, endpoint_query.redact_pii)

      case adaptor.execute_query(backend, query_args, opts) do
        {:ok, %QueryResult{rows: rows} = result} ->
          redacted_rows = PiiRedactor.redact_query_result(rows, redact_pii)
          {:ok, result |> Map.put(:rows, redacted_rows) |> Map.from_struct()}

        {:error, error} ->
          {:error, error}
      end
    end
  end

  @spec get_backend_for_query(Query.t()) :: {:ok, Backend.t()} | {:error, String.t()}
  defp get_backend_for_query(%Query{backend_id: backend_id}) when is_integer(backend_id) do
    case Backends.get_backend(backend_id) do
      %Backend{} = backend -> {:ok, backend}
      nil -> {:error, "Backend not found"}
    end
  end

  defp get_backend_for_query(%Query{user_id: user_id, language: language}) do
    backend_type = language_to_backend_type(language)
    find_backend_by_type_or_default(user_id, backend_type)
  end

  @spec language_to_backend_type(language()) :: atom() | nil
  defp language_to_backend_type(:pg_sql), do: :postgres
  defp language_to_backend_type(:ch_sql), do: :clickhouse
  defp language_to_backend_type(:bq_sql), do: :bigquery
  defp language_to_backend_type(_), do: nil

  @spec find_backend_by_type_or_default(user_id :: integer(), backend_type :: atom() | nil) ::
          {:ok, Backend.t()} | {:error, String.t()}
  defp find_backend_by_type_or_default(user_id, backend_type)
       when is_integer(user_id) and is_atom_value(backend_type) do
    user_backends = Backends.list_backends_by_user_id(user_id)

    case Enum.find(user_backends, &(&1.type == backend_type)) do
      nil -> get_default_backend_for_user(user_id)
      backend -> {:ok, backend}
    end
  end

  defp find_backend_by_type_or_default(user_id, nil) when is_integer(user_id) do
    get_default_backend_for_user(user_id)
  end

  @spec get_default_backend_for_user(user_id :: integer()) ::
          {:ok, Backend.t()} | {:error, String.t()}
  defp get_default_backend_for_user(user_id) when is_integer(user_id) do
    user = Users.Cache.get(user_id)
    {:ok, Backends.get_default_backend(user)}
  end

  @spec build_transformation_context(Backend.t()) :: map()
  defp build_transformation_context(%Backend{} = backend) do
    context = %{}

    context =
      if SingleTenant.supabase_mode?() and SingleTenant.postgres_backend?() do
        schema_prefix = Keyword.get(SingleTenant.postgres_backend_adapter_opts(), :schema)
        Map.put(context, :schema_prefix, schema_prefix)
      else
        context
      end

    Map.put(context, :backend_config, backend.config)
  end
end
