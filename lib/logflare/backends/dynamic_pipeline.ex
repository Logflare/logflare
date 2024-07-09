defmodule Logflare.Backends.DynamicPipeline do
  @moduledoc """
  Dynamically scales a Broadway pipeline transparently such that it does not need to care about scaling.


  Options:
  - `:name` - name of the pipeline, required.
  - `:pipeline` - module of the pipeline, required.
  - `:pipeline_args` - args of the pipeline, optional.
  - `:max_buffer_len` - soft limit that each pipeline buffer grows to before a new pipeline is added. Optional.
  - `:max_pipelines` - max pipelines that can be scaled to.
  - `:min_pipelines` - max pipelines that can be scaled to, defaults to 0.
  - `:resolve_count` - anonymous 1-arity function to determine what number of pipelines to scale to.
  - `:resolve_interval` - interval that the resolve_count will be checked.
  """
  use Supervisor
  alias __MODULE__.Coordinator

  require Logger

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
        # genstage default https://hexdocs.pm/gen_stage/GenStage.html#content
        max_buffer_len: 10_000,
        max_pipelines: System.schedulers_online(),
        min_pipelines: 0,
        resolve_count: fn _state -> 0 end,
        resolve_interval: 5_000,
        buffers: %{},
        last_count_increase: nil,
        last_count_decrease: nil
      })

    {_type, num_to_start, coordinator_state} = resolve_pipeline_count(state, 0)

    pipeline_specs =
      for i <- 0..num_to_start, i != 0 do
        child_spec(state, make_ref())
      end

    children =
      [
        {Agent, fn -> state end},
        {Coordinator, coordinator_state}
      ] ++ pipeline_specs

    Logger.debug("Started up DynamicPipeline")
    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Resolves desired number of pipelines from state
  """
  def resolve_pipeline_count(state, current) do
    args =
      Map.take(state, [:last_count_increase, :last_count_decrease])
      |> Map.put(:pipeline_count, current)

    desired = max(state.resolve_count.(args), state.min_pipelines)

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
    pid =
      Supervisor.which_children(name)
      |> Enum.find(fn
        {_id, _child, _type, [Agent]} -> true
        _ -> false
      end)
      |> elem(1)

    Agent.get(pid, fn v -> v end)
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
  """
  @spec remove_pipeline(tuple()) :: {:ok, integer(), tuple()} | {:error, :min_pipelines}
  def remove_pipeline(name) do
    count = pipeline_count(name)
    state = get_state(name)
    maybe_remove_pipeline(name, count, state)
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

  defp maybe_remove_pipeline(name, _count, _state) do
    id =
      list_pipelines(name)
      |> Enum.random()

    with :ok <- Supervisor.terminate_child(name, id),
         :ok <- Supervisor.delete_child(name, id) do
      count = pipeline_count(name)
      Logger.debug("DynamicPipeline - Removed pipeline #{inspect(id)}, count is now #{count}")
      {:ok, count, id}
    end
  end

  defp child_spec(%{pipeline: pipeline, name: name, pipeline_args: pipeline_args}, ref) do
    sharded_name = sup_name_to_pipeline_name(name, ref)
    new_args = pipeline_args ++ [name: sharded_name]
    %{id: sharded_name, start: {pipeline, :start_link, [new_args]}}
  end

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

    @impl GenServer
    def handle_info(:check, state) do
      pipelines = DynamicPipeline.list_pipelines(state.name)
      pipeline_count = Enum.count(pipelines)

      state =
        case DynamicPipeline.resolve_pipeline_count(state, pipeline_count) do
          {:incr, desired_count, new_state} ->
            diff = desired_count - pipeline_count

            for _ <- 1..diff do
              DynamicPipeline.add_pipeline(state.name)
            end

            new_state

          {:decr, desired_count, new_state} ->
            diff = pipeline_count - desired_count

            for _pipeline <- 1..diff do
              DynamicPipeline.remove_pipeline(state.name)
            end

            new_state

          {:noop, _desired, new_state} ->
            new_state
        end

      loop(state)
      {:noreply, state}
    end

    defp loop(args) do
      Process.send_after(self(), :check, args.resolve_interval)
    end
  end
end
