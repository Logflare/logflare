defmodule Logflare.Backends.DynamicPipeline do
  @moduledoc """
  Dynamically scales a Broadway pipeline transparently such that it does not need to care about scaling.
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
        min_idle_shutdown_after: 150_000,
        idle_shutdown_after: 30_000,
        touch: %{}
      })

    pipeline_specs =
      for i <- 0..state.min_pipelines, i != 0 do
        child_spec(state, make_ref())
      end

    touch_record = for spec <- pipeline_specs, into: %{}, do: {spec.id, NaiveDateTime.utc_now()}
    state = Map.put(state, :touch, touch_record)

    children =
      [
        {Agent, fn -> state end},
        {Coordinator, state}
      ] ++ pipeline_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  def get_state(name) do
    pid =
      Supervisor.which_children(name)
      |> Enum.find(fn
        {_id, _child, _type, [Agent]} -> true
        {Coordinator, _child, _type, _} -> false
        _ -> false
      end)
      |> elem(1)

    Agent.get(pid, fn v -> v end)
  end

  def touch_pipeline(pipeline_name) do
    sup_name = pipeline_name_to_sup_name(pipeline_name)

    pid =
      sup_name
      |> Supervisor.which_children()
      |> Enum.find(fn
        {_id, _child, _type, [Agent]} -> true
        _ -> false
      end)
      |> elem(1)

    pipelines = list_pipelines(sup_name)

    Agent.cast(pid, fn %{touch: touch_records} = v ->
      new_touch =
        for {k, v} <- Map.put(touch_records, pipeline_name, NaiveDateTime.utc_now()),
            k in pipelines,
            into: %{} do
          {k, v}
        end

      %{
        v
        | touch: new_touch
      }
    end)
  end

  def push_messages(name, messages) do
    %{pipeline: _pipeline} = state = get_state(name)

    # if buffers_full?(name) do
    #   add_pipeline(name)
    # end
    buffer_lens = buffer_len_by_pipeline(name)

    eligible =
      buffer_lens
      |> Enum.filter(fn {_id, num} -> num < state.max_buffer_len end)

    if Enum.empty?(eligible) and length(Map.keys(buffer_lens)) >= state.max_pipelines do
      {:error, :buffer_full}
    else
      for {id, num} <- Enum.sort_by(buffer_lens, fn {_, num} -> num end), reduce: messages do
        [] ->
          []

        acc ->
          to_take = state.max_buffer_len - num
          {taken, rem} = Enum.split(acc, to_take)
          Broadway.push_messages(id, taken)
          touch_pipeline(id)
          rem
      end
    end
    |> case do
      {:error, _} = err ->
        err

      [] ->
        :ok

      rem ->
        # add a new pipeline
        with {:ok, _count, pipeline_id} <- Coordinator.sync_add_pipeline(name) do
          Broadway.push_messages(pipeline_id, rem)
          touch_pipeline(pipeline_id)
          :ok
        end
    end
  end

  @doc """
  Adds a shard to a given DynamicPipeline

  """
  @spec add_pipeline(tuple()) :: {:ok, non_neg_integer(), tuple()}
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
        touch_pipeline(spec.id)
        count = pipeline_count(name)

        Logger.debug(
          "DynamicPipeline - Added pipeline #{inspect(spec.id)}, count is now #{count}"
        )

        {:ok, count, spec.id}

      err ->
        err
    end
  end

  def remove_pipeline(name) do
    count = pipeline_count(name)
    state = get_state(name)
    maybe_remove_pipeline(name, count, state)
  end

  def force_remove_pipeline(name) do
    count = pipeline_count(name)
    maybe_remove_pipeline(name, count, %{})
  end

  defp maybe_remove_pipeline(_name, count, %{min_pipelines: min}) when min >= count,
    do: {:error, :min_pipelines}

  defp maybe_remove_pipeline(_name, count, _state) when count == 0,
    do: {:error, :min_pipelines}

  defp maybe_remove_pipeline(name, _count, _state) do
    {id, _} =
      buffer_len_by_pipeline(name)
      |> Enum.sort_by(fn {_id, len} -> len end)
      |> List.first()

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

  def sup_name_to_pipeline_name({:via, module, {registry, identifier}}, shard) do
    {:via, module, {registry, {__MODULE__, identifier, shard}}}
  end

  def pipeline_name_to_sup_name({:via, module, {registry, {__MODULE__, identifier, _shard}}}) do
    {:via, module, {registry, identifier}}
  end

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
  Total buffer length of all sharded pipelines
  """
  def buffer_len(name) do
    buffer_len_by_pipeline(name)
    |> Map.values()
    |> Enum.sum()
  end

  def list_pipelines(name) do
    for {id, _child, _type, _mod} <- Supervisor.which_children(name),
        id != Agent and id != Coordinator do
      id
    end
  end

  @doc """
  Get buffer lengths of each sharded pipeline
  """
  def buffer_len_by_pipeline(name) do
    for {id, _child, _type, _mod} <- Supervisor.which_children(name),
        id != Agent and id != Coordinator,
        producer_name <- Broadway.producer_names(id) do
      Task.async(fn ->
        {
          id,
          GenStage.estimate_buffered_count(producer_name)
        }
      end)
    end
    |> Task.await_many()
    |> Enum.into(%{})
  end

  def healthy?(name) do
    with pid when is_pid(pid) <- whereis(name) do
      state = get_state(name)

      buffer_len_by_pipeline(name)
      |> Map.values()
      |> case do
        [] ->
          true

        lens ->
          Enum.any?(lens, fn v -> state.max_buffer_len > v end)
      end
    else
      _ -> false
    end
  end

  def whereis(name), do: GenServer.whereis(name)

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

    def init(%{} = args) do
      loop(args)
      {:ok, args}
    end

    def sync_add_pipeline(sup_name) do
      DynamicPipeline.find_coordinator_name(sup_name)
      |> GenServer.call(:add_pipeline)
    end

    def handle_call(:add_pipeline, _caller, state) do
      res = DynamicPipeline.add_pipeline(state.name)
      {:reply, res, state}
    end

    def handle_info(:check, state) do
      agent_state = DynamicPipeline.get_state(state.name)
      pipelines = DynamicPipeline.list_pipelines(state.name)
      touch_record = Map.get(agent_state, :touch)

      to_close =
        for {k, last_touched} <- touch_record,
            k in pipelines,
            NaiveDateTime.diff(NaiveDateTime.utc_now(), last_touched, :millisecond) >
              state.idle_shutdown_after do
          # should shutdown
          k
        end

      if length(to_close) > 0 and pipelines > state.min_pipelines do
        # has excess, close the excess
        diff = length(to_close) - state.min_pipelines

        for d <- 1..diff do
          DynamicPipeline.remove_pipeline(state.name)
        end
      end

      # refresh the list of pipelines
      pipelines = DynamicPipeline.list_pipelines(state.name)

      # find inactive pipelines
      min_to_close =
        for {k, last_touched} <- touch_record,
            k in pipelines,
            NaiveDateTime.diff(NaiveDateTime.utc_now(), last_touched, :millisecond) >
              state.min_idle_shutdown_after do
          # should shutdown
          k
        end

      if length(min_to_close) > 0 and length(pipelines) <= state.min_pipelines do
        for _d <- 1..length(min_to_close) do
          DynamicPipeline.force_remove_pipeline(state.name)
        end
      end

      loop(state)
      {:noreply, state}
    end

    defp loop(args) do
      next_interval =
        min(args.min_idle_shutdown_after, args.idle_shutdown_after)
        |> min(5_000)

      Process.send_after(self(), :check, next_interval)
    end
  end
end
