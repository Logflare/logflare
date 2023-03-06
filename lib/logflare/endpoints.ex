defmodule Logflare.Endpoints do
  @moduledoc false
  alias Logflare.Endpoints.Query
  alias Logflare.Endpoints.Resolver
  alias Logflare.Endpoints.Cache
  alias Logflare.Repo
  alias Logflare.User
  import Ecto.Query

  @spec count_endpoints_by_user(User.t() | integer()) :: integer()
  def count_endpoints_by_user(%User{id: user_id}), do: count_endpoints_by_user(user_id)

  def count_endpoints_by_user(user_id) do
    from(s in Query, where: s.user_id == ^user_id)
    |> Repo.aggregate(:count)
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
  def get_query_by_token(token) when is_binary(token) do
    get_by(token: token)
  end

  def get_mapped_query_by_token(token) when is_binary(token) do
    token
    |> get_query_by_token()
    |> case do
      nil -> nil
      query -> Query.map_query(query)
    end
  end

  @spec get_by(Keyword.t()) :: Query.t() | nil
  def get_by(kw) do
    Repo.get_by(Query, kw)
  end

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
    query
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> Repo.update()
  end

  @spec delete_query(Query.t()) :: {:ok, Query.t()} | {:error, any()}
  def delete_query(query) do
    Repo.delete(query)
  end

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
  @typep run_query_return :: {:ok, %{rows: [map()]}} | {:error, String.t()}
  @spec run_query(Query.t(), params :: map()) :: run_query_return()
  def run_query(
        %Query{query: query_string, user_id: user_id, sandboxable: sandboxable} = endpoint_query,
        params \\ %{}
      ) do
    with {:ok, declared_params} <- Logflare.SqlV2.parameters(query_string),
         sql_param <- Map.get(params, "sql"),
         transform_input =
           if(sandboxable && sql_param,
             do: {query_string, sql_param},
             else: query_string
           ),
         {:ok, transformed_query} <- Logflare.SqlV2.transform(transform_input, user_id),
         {:ok, result} <-
           exec_sql_on_bq(endpoint_query, transformed_query, declared_params, params) do
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

    query = %Query{
      query: query_string,
      sandboxable: opts.sandboxable,
      user: user,
      user_id: user.id
    }

    run_query(query, opts.params)
  end

  @doc """
  Runs a cached query.
  """
  @spec run_cached_query(Query.t(), map()) :: run_query_return()
  def run_cached_query(query, params \\ %{}) do
    Resolver.resolve(query, params)
    |> Cache.query()
  end

  defp exec_sql_on_bq(%Query{} = endpoint_query, transformed_query, declared_params, input_params)
       when is_binary(transformed_query) and
              is_list(declared_params) and
              is_map(input_params) do
    bq_params =
      Enum.map(declared_params, fn x ->
        %{
          name: x,
          parameterValue: %{
            value: input_params[x]
          },
          parameterType: %{
            type: "STRING"
          }
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

  defp process_bq_message(message, _user_id) when is_atom(message) do
    message
  end

  defp process_bq_message(message, user_id) when is_binary(message) do
    regex =
      ~r/#{env_project_id()}\.#{user_id}_#{env()}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/

    names = Regex.named_captures(regex, message)

    case names do
      %{"uuid" => uuid} ->
        uuid = String.replace(uuid, "_", "-")

        query =
          from s in Logflare.Source,
            where: s.token == ^uuid and s.user_id == ^user_id,
            select: s.name

        case Logflare.Repo.one(query) do
          nil ->
            message

          name ->
            Regex.replace(regex, message, name)
        end

      _ ->
        message
    end
  end
end
