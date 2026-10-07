defmodule Logflare.Backends.DynamicPipeline do
  @moduledoc """
  Dynamically scales a Broadway pipeline transparently such that it does not need to care about scaling.


  Options:
  - `:name` - name of the pipeline, required.
  - `:pipeline` - module of the pipeline, required.
  - `:pipeline_args` - args of the pipeline, optional.
  - `:max_pipelines` - max pipelines that can be scaled to.
  - `:min_pipelines` - max pipelines that can be scaled to, defaults to 0.
  - `:initial_count` - the initial number of pipelines to start. Defaults to :min_pipelines value.
  - `:resolve_count` - anonymous 1-arity function to determine what number of pipelines to scale to.
  - `:resolve_interval` - interval that the resolve_count will be checked.
  - `:shutdown` - how long each pipeline gets to drain on termination before it is killed. Defaults to 5_000.
  """
  use Supervisor

  alias __MODULE__.Coordinator
  alias Logflare.Backends.IngestEventQueue

  require Logger

  @finish_removal_delay_ms if Application.compile_env(:logflare, :env) == :test,
                             do: 50,
                             else: 1_000

  @replace_timeout_ms 30_000
  @default_shutdown_ms 5_000

  @type state :: %{
          name: term(),
          pipeline: module(),
          pipeline_args: keyword(),
          max_pipelines: pos_integer(),
          min_pipelines: non_neg_integer(),
          initial_count: non_neg_integer(),
          resolve_count: (map() -> non_neg_integer()),
          resolve_interval: pos_integer(),
          shutdown: timeout(),
          last_count_increase: NaiveDateTime.t() | nil,
          last_count_decrease: NaiveDateTime.t() | nil
        }

  @resolve_interval if Application.compile_env(:logflare, :env) == :test, do: 500, else: 5_000

  @spec start_link(args :: keyword()) :: Supervisor.on_start()
  def start_link(args) do
    name = Keyword.get(args, :name)
    Supervisor.start_link(__MODULE__, args, name: name)
  end

  def init(args) do
    state =
      Enum.into(args, %{
        name: nil,
        pipeline: nil,
        pipeline_args: [],
        max_pipelines: System.schedulers_online(),
        min_pipelines: 0,
        initial_count: args[:min_pipelines] || 0,
        resolve_count: fn _state -> 0 end,
        resolve_interval: @resolve_interval,
        shutdown: @default_shutdown_ms,
        last_count_increase: nil,
        last_count_decrease: nil
      })

    pipeline_specs =
      for i <- 0..state.initial_count, i != 0 do
        child_spec(state, make_ref())
      end

    children =
      [
        {Agent, fn -> state end},
        {Coordinator, state}
      ] ++ pipeline_specs

    Logger.debug("Started up DynamicPipeline")
    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Resolves desired number of pipelines from state
  """
  @spec resolve_pipeline_count(state :: state(), current :: non_neg_integer()) ::
          {:incr | :decr | :noop, non_neg_integer(), state()}
  def resolve_pipeline_count(state, current) do
    args =
      Map.take(state, [:last_count_increase, :last_count_decrease, :min_pipelines, :max_pipelines])
      |> Map.put(:pipeline_count, current)

    desired =
      try do
        max(state.resolve_count.(args), state.min_pipelines)
      rescue
        err ->
          fallback = max(current, state.min_pipelines)

          Logger.error(
            "Error when resolving DynamicPipeline counts, falling back to #{fallback} | err: #{inspect(err)}"
          )

          fallback
      end

    Logger.debug("DynamicPipeline desired pipeline count: #{desired}")

    cond do
      desired > current ->
        new_state = %{state | last_count_increase: NaiveDateTime.utc_now()}
        {:incr, desired, new_state}

      desired < current ->
        new_state = %{state | last_count_decrease: NaiveDateTime.utc_now()}
        {:decr, desired, new_state}

      desired == current ->
        {:noop, desired, state}
    end
  end

  @doc """
  Retrieves the state of a pipeline, which is merged from the args of the pipeline.
  """
  @spec get_state(tuple()) :: map()
  def get_state(name) do
    name
    |> find_agent()
    |> Agent.get(fn v -> v end)
  end

  @spec find_agent(tuple()) :: pid()
  defp find_agent(name) do
    Supervisor.which_children(name)
    |> Enum.find(fn
      {_id, _child, _type, [Agent]} -> true
      _ -> false
    end)
    |> elem(1)
  end

  @doc """
  Adds a shard to a given DynamicPipeline.
  """
  @spec add_pipeline(tuple()) :: {:ok, non_neg_integer(), tuple()} | {:error, :max_pipelines}
  def add_pipeline(name) do
    count = pipeline_count(name)
    state = get_state(name)

    maybe_add_pipeline(name, count, state)
  end

  defp maybe_add_pipeline(_name, count, %{max_pipelines: max}) when max <= count,
    do: {:error, :max_pipelines}

  defp maybe_add_pipeline(name, _count, state) do
    spec = child_spec(state, make_ref())

    case Supervisor.start_child(name, spec) do
      {:ok, _pid} ->
        count = pipeline_count(name)

        Logger.debug(
          "DynamicPipeline - Added pipeline #{inspect(spec.id)}, count is now #{count}"
        )

        {:ok, count, spec.id}

      err ->
        err
    end
  end

  @doc """
  Removes a pipeline from a DynamicPipeline tree.

  Asynchronous: marks the chosen shard's queue draining and returns immediately; it's
  actually terminated `@finish_removal_delay_ms` later (see `finish_remove_pipeline/3`).
  """
  @spec remove_pipeline(tuple()) :: {:ok, :draining, tuple()} | {:error, atom()}
  def remove_pipeline(name) do
    state = get_state(name)
    count = effective_pipeline_count(name, state)
    maybe_remove_pipeline(name, count, state)
  end

  @doc false
  @spec effective_pipeline_count(tuple(), state() | map()) :: non_neg_integer()
  def effective_pipeline_count(name, state) do
    name |> list_pipelines() |> Enum.count(&(not already_draining?(&1, state)))
  end

  @doc """
  Forces the removal of a pipeline from a DynamicPipeline tree. Applies to min pipelines.
  """
  def force_remove_pipeline(name) do
    count = pipeline_count(name)
    maybe_remove_pipeline(name, count, %{})
  end

  defp maybe_remove_pipeline(_name, count, %{min_pipelines: min}) when min >= count,
    do: {:error, :min_pipelines}

  defp maybe_remove_pipeline(_name, count, _state) when count == 0,
    do: {:error, :min_pipelines}

  defp maybe_remove_pipeline(name, _count, state) do
    case removal_candidates(name, state) do
      [] ->
        {:error, :no_candidates}

      candidates ->
        id = Enum.random(candidates)
        drain_and_schedule_removal(name, id, state)
        {:ok, :draining, id}
    end
  end

  @doc """
  Replaces every pipeline with one started from `pipeline_args`.

  Each outgoing pipeline is drained through the same path as `remove_pipeline/1`, so its
  pending events move to a replacement instead of being destroyed with its queue.
  """
  @spec replace_pipelines(tuple(), keyword()) :: :ok | {:error, [{tuple(), term()}]}
  def replace_pipelines(name, pipeline_args) when is_list(pipeline_args) do
    name
    |> find_coordinator_name()
    |> GenServer.call({:replace_pipelines, pipeline_args}, @replace_timeout_ms)
  end

  @doc false
  @spec do_replace_pipelines(tuple(), keyword()) :: :ok | {:error, [{tuple(), term()}]}
  def do_replace_pipelines(name, pipeline_args) do
    state = get_state(name)
    new_state = %{state | pipeline_args: pipeline_args}
    :ok = name |> find_agent() |> Agent.update(fn _ -> new_state end)

    failures =
      for id <- removal_candidates(name, state),
          {:error, reason} <- [replace_pipeline(name, id, state, new_state)],
          do: {id, reason}

    if failures == [], do: :ok, else: {:error, failures}
  end

  @spec replace_pipeline(tuple(), tuple(), state(), state()) :: :ok | {:error, term()}
  defp replace_pipeline(name, id, state, new_state) do
    case Supervisor.start_child(name, child_spec(new_state, make_ref())) do
      {:ok, _pid} ->
        drain_and_schedule_removal(name, id, state)
        :ok

      {:error, error} ->
        reason = start_error_reason(error)

        Logger.error(
          "DynamicPipeline - failed to start replacement for #{inspect(id)}, keeping it. Error: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  @spec start_error_reason(term()) :: term()
  defp start_error_reason({reason, child}) when elem(child, 0) == :child, do: reason
  defp start_error_reason(reason), do: reason

  @spec drain_and_schedule_removal(tuple(), tuple(), state() | map()) :: reference()
  defp drain_and_schedule_removal(name, id, state) do
    case shard_sid_bid_pid(id, state) do
      nil -> :ok
      sid_bid_pid -> IngestEventQueue.mark_draining(sid_bid_pid)
    end

    coordinator = find_coordinator_name(name)
    Process.send_after(coordinator, {:finish_remove_pipeline, id}, @finish_removal_delay_ms)
  end

  defp removal_candidates(name, state) do
    for id <- list_pipelines(name), not already_draining?(id, state), do: id
  end

  defp already_draining?(id, state) do
    case shard_sid_bid_pid(id, state) do
      nil -> false
      sid_bid_pid -> IngestEventQueue.draining?(sid_bid_pid)
    end
  end

  @doc false
  @spec finish_remove_pipeline(tuple(), tuple(), state()) :: :ok
  def finish_remove_pipeline(name, id, state) do
    move_remaining_to_survivor(name, id, state)

    try do
      with :ok <- Supervisor.terminate_child(name, id),
           :ok <- Supervisor.delete_child(name, id) do
        Logger.debug(
          "DynamicPipeline - Removed pipeline #{inspect(id)}, count is now #{pipeline_count(name)}"
        )
      end
    rescue
      e ->
        Logger.error(
          "Error when attempting to terminate and remove pipeline. Error: #{Exception.format(:error, e, __STACKTRACE__)}"
        )
    end

    :ok
  end

  defp move_remaining_to_survivor(_name, id, state) do
    with {_, _, doomed_pid} = sid_bid_pid <- shard_sid_bid_pid(id, state),
         queues_key when queues_key != nil <- pipeline_args_sid_bid(state[:pipeline_args] || []),
         [_ | _] = survivors <- survivor_targets(queues_key, doomed_pid) do
      move_to_any_survivor(sid_bid_pid, survivors, id)
    else
      _ -> :ok
    end
  end

  defp move_to_any_survivor(_sid_bid_pid, [], _id), do: :ok

  defp move_to_any_survivor(sid_bid_pid, survivors, id) do
    {survivor_key, _tid} = candidate = Enum.random(survivors)

    case IngestEventQueue.move(sid_bid_pid, survivor_key) do
      {:ok, 0} ->
        :ok

      {:ok, moved} ->
        Logger.debug(
          "DynamicPipeline - moved #{moved} pending pointer(s) from #{inspect(id)} before removal"
        )

      {:error, :not_initialized} ->
        move_to_any_survivor(sid_bid_pid, survivors -- [candidate], id)
    end
  end

  defp survivor_targets(queues_key, doomed_pid) do
    targets =
      queues_key
      |> IngestEventQueue.list_queues_with_tids()
      |> Enum.reject(fn {table_key, _tid} -> elem(table_key, 2) == doomed_pid end)

    case Enum.filter(targets, fn {table_key, _tid} -> elem(table_key, 2) != nil end) do
      [] -> targets
      live -> live
    end
  end

  defp shard_sid_bid_pid(id, state) do
    producer_pid = id |> Broadway.producer_names() |> List.first() |> GenServer.whereis()

    case {producer_pid, pipeline_args_sid_bid(state[:pipeline_args] || [])} do
      {nil, _} -> nil
      {_pid, nil} -> nil
      {pid, {:consolidated, bid}} -> {:consolidated, bid, pid}
      {pid, {sid, bid}} -> {sid, bid, pid}
    end
  end

  defp pipeline_args_sid_bid(pipeline_args) do
    case {Keyword.get(pipeline_args, :source), Keyword.get(pipeline_args, :backend)} do
      {%{id: sid}, %{id: bid}} -> {sid, bid}
      {nil, %{id: bid}} -> {:consolidated, bid}
      _ -> nil
    end
  end

  @spec child_spec(state :: state(), ref :: reference()) :: Supervisor.child_spec()
  defp child_spec(
         %{pipeline: pipeline, name: name, pipeline_args: pipeline_args, shutdown: shutdown},
         ref
       ) do
    sharded_name = sup_name_to_pipeline_name(name, ref)
    new_args = pipeline_args ++ [name: sharded_name]
    %{id: sharded_name, start: {pipeline, :start_link, [new_args]}, shutdown: shutdown}
  end

  @spec ack(via :: term(), successful :: list(), failed :: list()) :: :ok
  def ack(_via, _successful, _failed) do
    :ok
  end

  @doc """
  Convert a DynamicPipeline name to a pipeline name
  """
  @spec sup_name_to_pipeline_name(tuple(), term()) :: tuple()
  def sup_name_to_pipeline_name({:via, module, {registry, identifier}}, shard) do
    {:via, module, {registry, {__MODULE__, identifier, shard}}}
  end

  @doc """
  Convert a pipeline name to a DynamicPipeline sup name
  """
  @spec pipeline_name_to_sup_name(tuple()) :: tuple()
  def pipeline_name_to_sup_name({:via, module, {registry, {__MODULE__, identifier, _shard}}}) do
    {:via, module, {registry, identifier}}
  end

  @doc """
  Counts the number of pipelines in use.
  """
  @spec pipeline_count(tuple()) :: non_neg_integer()
  def pipeline_count(name) do
    Supervisor.which_children(name)
    |> Enum.filter(fn
      {_, _, _, [Agent]} -> false
      {Coordinator, _, _, _} -> false
      _ -> true
    end)
    |> length()
  end

  @doc """
  Lists the proc names of the DynamicPipeline sup tree.
  """
  @spec list_pipelines(tuple()) :: [tuple()]
  def list_pipelines(name) do
    for {id, _child, _type, _mod} <- Supervisor.which_children(name),
        id != Agent and id != Coordinator do
      id
    end
  end

  @doc """
  Returns the pid of a given DynamicPipeline name or pipeline name.
  """
  @spec whereis(term()) :: pid() | nil
  def whereis(name), do: GenServer.whereis(name)

  @doc """
  Returns the pid of a DynamicPipeline tree coordinator
  """
  @spec find_coordinator_name(tuple()) :: pid()
  def find_coordinator_name(sup_name) do
    Supervisor.which_children(sup_name)
    |> Enum.find(fn
      {Coordinator, _, _, _} -> true
      _ -> false
    end)
    |> elem(1)
  end

  defmodule Coordinator do
    @moduledoc """
    Coordinates the starting up and shutting down of pipelines.
    """
    use GenServer

    alias Logflare.Backends.DynamicPipeline

    @spec start_link(args :: DynamicPipeline.state()) :: GenServer.on_start()
    def start_link(args) do
      GenServer.start_link(__MODULE__, args)
    end

    @impl GenServer
    def init(%{} = args) do
      loop(args)
      {:ok, args}
    end

    @doc """
    Syncronously adds a pipeline. Blocks.
    """
    @spec sync_add_pipeline(tuple()) ::
            {:ok, non_neg_integer(), tuple()} | {:error, :max_pipelines}
    def sync_add_pipeline(sup_name) do
      DynamicPipeline.find_coordinator_name(sup_name)
      |> GenServer.call(:add_pipeline)
    end

    @impl GenServer
    def handle_call(:add_pipeline, _caller, state) do
      res = DynamicPipeline.add_pipeline(state.name)
      {:reply, res, state}
    end

    def handle_call({:replace_pipelines, pipeline_args}, _caller, state) do
      res = DynamicPipeline.do_replace_pipelines(state.name, pipeline_args)
      {:reply, res, %{state | pipeline_args: pipeline_args}}
    end

    @impl GenServer
    def handle_info({:finish_remove_pipeline, id}, state) do
      DynamicPipeline.finish_remove_pipeline(state.name, id, state)
      {:noreply, state}
    end

    @impl GenServer
    def handle_info(:check, state) do
      pipelines = DynamicPipeline.list_pipelines(state.name)
      pipeline_count = Enum.count(pipelines)

      :telemetry.execute(
        [:logflare, :backends, :dynamic_pipeline],
        %{pipeline_count: pipeline_count},
        build_telemetry_metadata(state)
      )

      effective_count = DynamicPipeline.effective_pipeline_count(state.name, state)

      state =
        case DynamicPipeline.resolve_pipeline_count(state, effective_count) do
          {:incr, desired_count, new_state} ->
            diff = desired_count - effective_count

            for _ <- 1..diff do
              DynamicPipeline.add_pipeline(state.name)
            end
            |> do_telemetry(:increment, state, pipeline_count)

            new_state

          {:decr, desired_count, new_state} ->
            diff = effective_count - desired_count

            for _pipeline <- 1..diff do
              DynamicPipeline.remove_pipeline(state.name)
            end
            |> do_telemetry(:decrement, state, pipeline_count)

            new_state

          {:noop, _desired, new_state} ->
            new_state
        end

      loop(state)
      {:noreply, state}
    end

    @spec loop(state :: DynamicPipeline.state()) :: reference()
    defp loop(args) do
      # add small randomizer to spread out resolve checks
      randomizer = :rand.uniform(ceil(args.resolve_interval / 5))
      Process.send_after(self(), :check, args.resolve_interval + randomizer)
    end

    @spec do_telemetry(
            actions :: list(),
            action :: :increment | :decrement,
            state :: DynamicPipeline.state(),
            from_pipeline_count :: non_neg_integer()
          ) :: :ok
    defp do_telemetry(actions, action, state, from_pipeline_count) do
      error_count =
        Enum.count(actions, fn
          {:error, _} -> true
          _ -> false
        end)

      :telemetry.execute(
        [:logflare, :backends, :dynamic_pipeline, action],
        %{
          error_count: error_count,
          success_count: length(actions) - error_count,
          from_pipeline_count: from_pipeline_count
        },
        build_telemetry_metadata(state)
      )
    end

    @spec build_telemetry_metadata(state :: DynamicPipeline.state()) :: %{
            backend_id: pos_integer(),
            backend_token: atom(),
            backend_type: atom(),
            source_id: pos_integer() | nil,
            source_token: atom() | nil,
            consolidated: boolean() | nil
          }
    defp build_telemetry_metadata(state) do
      source = state.pipeline_args[:source]
      backend = state.pipeline_args[:backend]

      base = %{
        backend_id: backend.id,
        backend_token: backend.token,
        backend_type: backend.type
      }

      if source do
        Map.merge(base, %{
          source_id: source.id,
          source_token: source.token
        })
      else
        Map.put(base, :consolidated, true)
      end
    end
  end
end
