defmodule Logflare.Endpoint.Cache do

    def resolve(%Logflare.Endpoint.Query{id: id} = query) do
        :global.set_lock({__MODULE__, id})
        result =
        case :global.whereis_name({__MODULE__, id}) do
            :undefined ->
                {:ok, pid } = DynamicSupervisor.start_child(__MODULE__, {__MODULE__, query})
                pid
            pid ->
                pid
        end
        :global.del_lock({__MODULE__, id})
        result
    end

    use GenServer

    @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
    @max_results 10_000
    @ttl_secs 60 # seconds until cache is invalidated
    @inactivity_minutes 60 # minutes until the Cache process is terminated

    import Ecto.Query, only: [from: 2]

    def start_link(query) do
        GenServer.start_link(__MODULE__, query, name: {:global, {__MODULE__, query.id}})
    end

    defstruct query: nil, last_query_at: nil, last_update_at: nil, cached_result: nil

    def init(query) do
        {:ok, %__MODULE__{query: query}}
    end

    def handle_call({:query, params}, _from, %__MODULE__{cached_result: nil} = state) do
      state = %{state | last_query_at: DateTime.utc_now()}
      do_query(params, state)
    end

    def handle_call({:query, %{}}, _from, %__MODULE__{} = state) do
      state = %{state | last_query_at: DateTime.utc_now()}
      {:reply, {:ok, state.cached_result}, state, timeout_until_fetching(state)}
    end

    def handle_info(:timeout, state) do
      now = DateTime.utc_now()
      if DateTime.diff(now, state.last_query_at || now, :second) >= @inactivity_minutes * 60 do
        {:stop, :normal, state}
      else
        {:ok, parameters} = Logflare.SQL.parameters(state.query.query)
        if Enum.empty?(parameters) do
          {:reply, _, state, timeout} = do_query([], state)
          {:noreply, state, timeout}
        else
          {:noreply, state}
        end
      end
    end

    defp timeout_until_fetching(state) do
      max(0, @ttl_secs - DateTime.diff(DateTime.utc_now(), state.last_update_at, :second)) * 1000
    end


    defp do_query(params, state) do
        # Ensure latest version of the query is used
        state = %{state | query: Logflare.Repo.reload(state.query),
                          last_update_at: DateTime.utc_now() }
        case Logflare.SQL.parameters(state.query.query) do
           {:ok, parameters} ->
             case Logflare.SQL.transform(state.query.query, state.query.user_id) do
               {:ok, query} ->
                 params = Enum.map(parameters, fn x  ->
                   %{
                     name: x,
                     parameterValue: %{
                       value: params[x],
                     },
                     parameterType: %{
                       type: "STRING",
                     }
                    }
                 end)

                 case Logflare.BqRepo.query_with_sql_and_params(@project_id, query, params,
                                           parameterMode: "NAMED", maxResults: @max_results) do
                       {:ok, result} ->
                          if Enum.empty?(params) do
                            # Cache the result (no parameters)
                            state = %{state | cached_result: result}
                            {:reply, {:ok, result}, state, timeout_until_fetching(state)}
                          else
                            # Uncacheable result
                            {:reply, {:ok, result}, state}
                          end
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

    def query(cache, params) do
        GenServer.call(cache, {:query, params}, :infinity)
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
      regex = ~r/#{@project_id}\.#{user_id}_#{Mix.env}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/
      names = Regex.named_captures(regex, message)
      case names do
        %{"uuid" => uuid} ->
          uuid = String.replace(uuid, "_", "-")
          query = from s in Logflare.Source,
                  where: s.token == ^uuid and s.user_id == ^user_id,
                  select: s.name
          case Logflare.Repo.one(query) do
            nil -> message
            name ->
              Regex.replace(regex, message, name)
          end
        _ -> message
      end
    end


end