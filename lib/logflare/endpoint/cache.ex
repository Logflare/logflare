defmodule Logflare.Endpoint.Cache do
  # Find all processes for the query
  def resolve(%Logflare.Endpoint.Query{id: id} = query) do
    Enum.filter(:global.registered_names(), fn
      {__MODULE__, ^id, _} ->
        true

      _ ->
        false
    end)
    |> Enum.map(&:global.whereis_name/1)
  end

  # Find or spawn a (query * param) process
  def resolve(%Logflare.Endpoint.Query{id: id} = query, params) do
    :global.set_lock({__MODULE__, {id, params}})

    result =
      case :global.whereis_name({__MODULE__, id, params}) do
        :undefined ->
          {:ok, pid} = DynamicSupervisor.start_child(__MODULE__, {__MODULE__, {query, params}})
          pid

        pid ->
          pid
      end

    :global.del_lock({__MODULE__, {id, params}})
    result
  end

  use GenServer

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @max_results 10_000
  # minutes until the Cache process is terminated
  @inactivity_minutes 90
  @env Application.get_env(:logflare, :env)

  import Ecto.Query, only: [from: 2]

  def start_link({query, params}) do
    GenServer.start_link(__MODULE__, {query, params},
      name: {:global, {__MODULE__, query.id, params}}
    )
  end

  defstruct query: nil, params: %{}, last_query_at: nil, last_update_at: nil, cached_result: nil

  def init({query, params}) do
    {:ok, %__MODULE__{query: query, params: params} |> fetch_latest_query_endpoint() }
  end

  def handle_call(:query, _from, %__MODULE__{cached_result: nil} = state) do
    state = %{state | last_query_at: DateTime.utc_now()}
    do_query(state)
  end

  def handle_call(:query, _from, %__MODULE__{last_update_at: last_update_at} = state) do
    state = %{state | last_query_at: DateTime.utc_now()}
    if DateTime.diff(DateTime.utc_now(), last_update_at, :second) > state.query.cache_duration_seconds do
      do_query(state)
    else
      {:reply, {:ok, state.cached_result}, state, timeout_until_fetching(state)}
    end
  end

  def handle_call(:invalidate, _from, state) do
    {:reply, :ok, %{state | cached_result: nil}}
  end

  def handle_info(:timeout, state) do
    now = DateTime.utc_now()

    if DateTime.diff(now, state.last_query_at || now, :second) >= @inactivity_minutes * 60 do
      {:stop, :normal, state}
    else
      if state.query.proactive_requerying_seconds > 0 &&
         state.query.proactive_requerying_seconds - DateTime.diff(DateTime.utc_now(), state.last_update_at, :second) >= 0 do
        {:reply, _, state, timeout} = do_query(state)
        {:noreply, state, timeout}
      else
        {:noreply, state, timeout_until_fetching(state)}
      end
    end
  end

  defp timeout_until_fetching(state) do
    min(@inactivity_minutes * 60,
        max(0, state.query.proactive_requerying_seconds - DateTime.diff(DateTime.utc_now(), state.last_update_at, :second))) * 1000
  end

  defp fetch_latest_query_endpoint(state) do
    %{
      state
      | query:
          Logflare.Repo.reload(state.query)
          |> Logflare.Repo.preload(:user)
          |> Logflare.Endpoint.Query.map_query(),
        last_update_at: DateTime.utc_now()
    }
  end

  defp do_query(state) do
    # Ensure latest version of the query is used
    state = fetch_latest_query_endpoint(state)
    params = state.params

    case Logflare.SQL.parameters(state.query.query) do
      {:ok, parameters} ->
        query =
          if state.query.sandboxable && Map.get(params, "sql") do
            {state.query.query, Map.get(params, "sql")}
          else
            state.query.query
          end

        case Logflare.SQL.transform(query, state.query.user_id) do
          {:ok, query} ->
            params =
              Enum.map(parameters, fn x ->
                %{
                  name: x,
                  parameterValue: %{
                    value: params[x]
                  },
                  parameterType: %{
                    type: "STRING"
                  }
                }
              end)

            case Logflare.BqRepo.query_with_sql_and_params(
                   state.query.user,
                   state.query.user.bigquery_project_id || @project_id,
                   query,
                   params,
                   parameterMode: "NAMED",
                   maxResults: @max_results
                 ) do
              {:ok, result} ->
                # Cache the result (no parameters)
                state = %{state | cached_result: result}
                {:reply, {:ok, result}, state, timeout_until_fetching(state)}

              {:error, err} ->
                error = Jason.decode!(err.body)["error"] |> process_error(state.query.user_id)
                {:reply, {:error, error}, state}
            end

          {:error, err} ->
            {:reply, {:error, err}, state}
        end

      {:error, err} ->
        {:reply, {:error, err}, state}
    end
  end

  def query(cache) do
    GenServer.call(cache, :query, :infinity)
  end

  def invalidate(cache) do
    GenServer.call(cache, :invalidate)
  end

  defp process_error(error, user_id) do
    error = %{error | "message" => process_message(error["message"], user_id)}

    if is_list(error["errors"]) do
      %{error | "errors" => Enum.map(error["errors"], fn err -> process_error(err, user_id) end)}
    else
      error
    end
  end

  defp process_message(message, user_id) do
    regex =
      ~r/#{@project_id}\.#{user_id}_#{@env}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/

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
