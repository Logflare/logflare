defmodule Logflare.Endpoints do
  @moduledoc false
  alias Ecto.Adapters.SQL
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Endpoints.Cache
  alias Logflare.Endpoints.Query
  alias Logflare.Endpoints.Resolver
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Users
  alias Logflare.Utils

  import Ecto.Query
  @typep run_query_return :: {:ok, %{rows: [map()]}} | {:error, String.t()}

  @spec count_endpoints_by_user(User.t() | integer()) :: integer()
  def count_endpoints_by_user(%User{id: user_id}), do: count_endpoints_by_user(user_id)

  def count_endpoints_by_user(user_id) do
    q = from(s in Query, where: s.user_id == ^user_id)
    Repo.aggregate(q, :count)
  end

  @spec list_endpoints_by(keyword()) :: [Query.t()] | []
  def list_endpoints_by(kw) do
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
  @spec get_endpoint_query(integer()) :: Query.t() | nil
  def get_endpoint_query(id), do: Repo.get(Query, id)

  @spec get_query_by_token(binary()) :: Query.t() | nil
  def get_query_by_token(token) when is_binary(token), do: get_by(token: token)

  def get_mapped_query_by_token(token) when is_binary(token) do
    token
    |> get_query_by_token()
    |> then(fn
      nil -> nil
      query -> Query.map_query_sources(query)
    end)
  end

  @doc """
  Puts the `:query` key of the `Query` with the latest source mappings.
  This ensure that the query will have the latest source names (assuming a name change)
  """
  @spec map_query_sources(Query.t()) :: Query.t()
  def map_query_sources(endpoint), do: Query.map_query_sources(endpoint)

  @spec get_by(Keyword.t()) :: Query.t() | nil
  def get_by(kw), do: Repo.get_by(Query, kw)

  @spec create_query(User.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def create_query(user, params) do
    user
    |> Ecto.build_assoc(:endpoint_queries)
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> Repo.insert()
  end

  @doc """
  Creates a sandboxed endpoint. A sandboxed endpoint is an endpoint with a "parent" endpoint containing a CTE.

  This will allow us to query the parent sandbox using a fixed SQL query, without allowing unrestricted sql queries to be made.
  """
  @spec create_sandboxed_query(User.t(), Query.t(), map()) :: {:ok, Query.t()} | {:error, :no_cte}
  def create_sandboxed_query(user, sandbox, attrs) do
    case Logflare.SqlV2.contains_cte?(sandbox.query) do
      true ->
        user
        |> Ecto.build_assoc(:endpoint_queries, sandbox_query: sandbox)
        |> Repo.preload(:user)
        |> Query.sandboxed_endpoint_changeset(attrs)
        |> Repo.insert()

      false ->
        {:error, :no_cte}
    end
  end

  @spec update_query(Query.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def update_query(query, params) do
    with endpoint <- Repo.preload(query, :user),
         changeset <- Query.update_by_user_changeset(endpoint, params),
         {:ok, endpoint} <- Repo.update(changeset) do
      changed_keys = Map.keys(changeset.changes)

      should_kill_caches? =
        Enum.any?(changed_keys, fn key ->
          key in [
            :query,
            :sandboxable,
            :cache_duration_seconds,
            :proactive_requerying_seconds,
            :max_limit,
            :enable_auth
          ]
        end)

      if should_kill_caches? do
        # kill all caches
        for pid <- Resolver.resolve(endpoint) do
          Utils.Tasks.async(fn ->
            Cache.invalidate(pid)
          end)
        end
        |> Task.await_many(30_000)
      end

      {:ok, endpoint}
    end
  end

  @spec delete_query(Query.t()) :: {:ok, Query.t()} | {:error, any()}
  def delete_query(query), do: Repo.delete(query)

  @doc """
  Parses a query string (but does not run it)

  ### Example
    iex> parse_query_string("select @testing from date")
    {:ok, %{parameters: ["testing"]}}
  """
  @spec parse_query_string(String.t()) :: {:ok, %{parameters: [String.t()]}} | {:error, any()}
  def parse_query_string(query_string) do
    with {:ok, declared_params} <- Logflare.SqlV2.parameters(query_string) do
      {:ok, %{parameters: declared_params}}
    end
  end

  @doc """
  Runs a an endpoint query
  """
  @spec run_query(Query.t(), params :: map()) :: run_query_return()
  def run_query(%Query{} = endpoint_query, params \\ %{}) do
    %Query{query: query_string, user_id: user_id, sandboxable: sandboxable} = endpoint_query
    sql_param = Map.get(params, "sql")

    transform_input =
      if(sandboxable && sql_param, do: {query_string, sql_param}, else: query_string)

    {backend, sources} = Query.choose_backend_and_extract_sources(endpoint_query)

    with {:ok, declared_params} <- Logflare.SqlV2.parameters(query_string),
         {:ok, transformed_query} <- Logflare.SqlV2.transform(backend, transform_input, user_id),
         {:ok, result} <-
           run_on_backend(
             backend,
             sources,
             endpoint_query,
             transformed_query,
             declared_params,
             params
           ) do
      {:ok, result}
    end
  end

  @doc """
  Runs a query string

  ### Example
    iex> run_query_string(%User{...}, "select current_time() where @value > 4", params: %{"value" => "123"})
    {:ok, %{rows:  [...]} }
  """
  @typep run_query_string_opts :: [sandboxable: boolean(), params: map()]
  @spec run_query_string(User.t(), String.t(), run_query_string_opts()) :: run_query_return()
  def run_query_string(user, query_string, opts \\ %{}) do
    opts = Enum.into(opts, %{sandboxable: false, params: %{}})

    source_mapping =
      user
      |> Users.preload_sources()
      |> then(fn %{sources: sources} -> sources end)
      |> Enum.map(&{&1.name, &1.token})

    query = %Query{
      query: query_string,
      sandboxable: opts.sandboxable,
      user: user,
      user_id: user.id,
      source_mapping: source_mapping
    }

    run_query(query, opts.params)
  end

  @doc """
  Runs a cached query.
  """
  @spec run_cached_query(Query.t(), map()) :: run_query_return()
  def run_cached_query(query, params \\ %{}) do
    query
    |> Resolver.resolve(params)
    |> Cache.query()
  end

  defp exec_sql_on_bq(%Query{} = endpoint_query, transformed_query, declared_params, input_params)
       when is_binary(transformed_query) and
              is_list(declared_params) and
              is_map(input_params) do
    endpoint_query = Repo.preload(endpoint_query, :user)

    bq_params =
      Enum.map(declared_params, fn input_name ->
        %{
          name: input_name,
          parameterValue: %{value: input_params[input_name]},
          parameterType: %{type: "STRING"}
        }
      end)

    # execute the query on bigquery
    case Logflare.BqRepo.query_with_sql_and_params(
           endpoint_query.user,
           endpoint_query.user.bigquery_project_id || env_project_id(),
           transformed_query,
           bq_params,
           parameterMode: "NAMED",
           maxResults: endpoint_query.max_limit,
           location: endpoint_query.user.bigquery_dataset_location
         ) do
      {:ok, result} ->
        {:ok, result}

      {:error, %{body: body}} ->
        error = Jason.decode!(body)["error"] |> process_bq_error(endpoint_query.user_id)
        {:error, error}

      {:error, err} when is_atom(err) ->
        {:error, process_bq_error(err, endpoint_query.user_id)}
    end
  end

  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]
  defp env, do: Application.get_env(:logflare, :env)

  defp process_bq_error(error, user_id) when is_atom(error) do
    %{"message" => process_bq_message(error, user_id)}
  end

  defp process_bq_error(error, user_id) when is_map(error) do
    error = %{error | "message" => process_bq_message(error["message"], user_id)}

    if is_list(error["errors"]) do
      %{
        error
        | "errors" => Enum.map(error["errors"], fn err -> process_bq_error(err, user_id) end)
      }
    else
      error
    end
  end

  defp process_bq_message(message, _user_id) when is_atom(message), do: message

  defp process_bq_message(message, user_id) when is_binary(message) do
    regex =
      ~r/#{env_project_id()}\.#{user_id}_#{env()}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/

    case Regex.named_captures(regex, message) do
      %{"uuid" => uuid} ->
        uuid = String.replace(uuid, "_", "-")

        query =
          from(s in Logflare.Source,
            where: s.token == ^uuid and s.user_id == ^user_id,
            select: s.name
          )

        case Logflare.Repo.one(query) do
          nil -> message
          name -> Regex.replace(regex, message, name)
        end

      _ ->
        message
    end
  end

  defp run_on_backend(:bigquery, _, endpoint_query, transformed_query, declared_params, params),
    do: exec_sql_on_bq(endpoint_query, transformed_query, declared_params, params)

  defp run_on_backend(
         :postgres,
         [source],
         endpoint_query,
         transformed_query,
         declared_params,
         params
       ),
       do: exec_sql_on_pg(source, endpoint_query, transformed_query, declared_params, params)

  defp run_on_backend(:postgres, _, _, _, _, _),
    do: {:error, "Postgres does not support multiple sources"}

  defp exec_sql_on_pg(%{source_backends: [source_backend]}, _, transformed_query, _, input_params) do
    source_backend = Repo.preload(source_backend, :source)

    with repo <- PostgresAdaptor.create_repo(source_backend),
         :ok <- PostgresAdaptor.connect_to_repo(source_backend),
         {:ok, result} <- SQL.query(repo, transformed_query, Map.to_list(input_params)),
         %{columns: columns, rows: rows} <- result do
      rows = Enum.map(rows, fn row -> columns |> Enum.zip(row) |> Map.new() end)
      {:ok, %{rows: rows}}
    end
  end
end
