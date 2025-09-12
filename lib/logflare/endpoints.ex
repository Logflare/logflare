defmodule Logflare.Endpoints do
  @moduledoc false

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Alerting
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints.PiiRedactor
  alias Logflare.Endpoints.Query
  alias Logflare.Endpoints.Resolver
  alias Logflare.Endpoints.ResultsCache
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sql
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Utils

  @valid_sql_languages ~w(bq_sql ch_sql pg_sql)a

  @typep language :: :bq_sql | :ch_sql | :pg_sql | :lql
  @typep run_query_return :: {:ok, %{rows: [map()]}} | {:error, String.t()}

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
    user
    |> Ecto.build_assoc(:endpoint_queries)
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> Repo.insert()
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

  @spec update_query(Query.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def update_query(%Query{} = query, params) when is_map(params) do
    with endpoint <- Repo.preload(query, :user),
         changeset <- Query.update_by_user_changeset(endpoint, params),
         {:ok, endpoint} <- Repo.update(changeset) do
      for pid <- Resolver.list_caches(endpoint) do
        Utils.Tasks.async(fn ->
          ResultsCache.invalidate(pid)
        end)
      end
      |> Task.await_many(30_000)

      {:ok, endpoint}
    end
  end

  @spec delete_query(Query.t()) :: {:ok, Query.t()} | {:error, any()}
  def delete_query(query), do: Repo.delete(query)

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
  def run_query(%Query{} = endpoint_query, params \\ %{}, opts \\ [])
      when is_map(params) and is_list(opts) do
    %Query{query: query_string, user_id: user_id, sandboxable: sandboxable} = endpoint_query
    sql_param = Map.get(params, "sql")

    endpoints =
      list_endpoints_by(user_id: endpoint_query.user_id)
      |> Enum.filter(&(&1.id != endpoint_query.id))

    alerts = Alerting.list_alert_queries_by_user_id(endpoint_query.user_id)

    with {:ok, declared_params} <- Sql.parameters(query_string),
         {:ok, expanded_query} <-
           Sql.expand_subqueries(
             endpoint_query.language,
             query_string,
             endpoints ++ alerts
           ),
         transform_input =
           if(sandboxable && sql_param, do: {expanded_query, sql_param}, else: expanded_query),
         {:ok, transformed_query} <-
           Sql.transform(endpoint_query.language, transform_input, user_id) do
      :telemetry.span(
        [:logflare, :endpoints, :run_query, :exec_query_on_backend],
        %{endpoint_id: endpoint_query.id, language: endpoint_query.language},
        fn ->
          result =
            exec_query_on_backend(
              endpoint_query,
              transformed_query,
              declared_params,
              params,
              opts
            )

          total_rows =
            case result do
              {:ok, %{rows: rows}} -> length(rows)
              _ -> 0
            end

          {result, %{total_rows: total_rows}}
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
      redact_pii: Keyword.get(opts, :redact_pii, false)
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
        {:ok, rows} when is_list(rows) ->
          redacted_rows = PiiRedactor.redact_query_result(rows, redact_pii)
          {:ok, %{rows: redacted_rows}}

        {:ok, %{rows: rows} = result} ->
          # Pass through the full result map with all metadata, but redact PII in rows
          redacted_rows = PiiRedactor.redact_query_result(rows, redact_pii)
          {:ok, %{result | rows: redacted_rows}}

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

  @doc """
  Clears all caches for an endpoint, including ResultsCache processes and Endpoints.Cache entries.
  """
  @spec clear_all_endpoint_caches(Query.t()) :: :ok
  def clear_all_endpoint_caches(%Query{} = endpoint) do
    # Kill all ResultsCache processes
    tasks =
      for pid <- Resolver.list_caches(endpoint) do
        Utils.Tasks.async(fn ->
          ResultsCache.invalidate(pid)
        end)
      end

    Task.await_many(tasks, 30_000)

    :ok
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
