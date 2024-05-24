defmodule Logflare.Backends.DynamicPipeline do
  @moduledoc """
  Dynamically scales a Broadway pipeline transparently such that it does not need to care about scaling.
  """
  use Supervisor

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
        min_pipelines: 0
      })

    pipeline_specs =
        for i <- 0..state.min_pipelines, i != 0 do
          child_spec(state, make_ref())
        end

    children =
      [
        {Agent, fn -> state end}
      ] ++ pipeline_specs

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp get_state(name) do
    pid =
      Supervisor.which_children(name)
      |> Enum.find(fn
        {_id, _child, _type, [Agent]} -> true
        _ -> false
      end)
      |> elem(1)

    Agent.get(pid, fn v -> v end)
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
        with {:ok, _count, pipeline_id} <- add_pipeline(name) do
          Broadway.push_messages(pipeline_id, rem)
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
        {:ok, pipeline_count(name), spec.id}

      err ->
        err
    end
  end

  def remove_pipeline(name) do
    count = pipeline_count(name)
    state = get_state(name)
    maybe_remove_pipeline(name, count, state)
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
      {:ok, pipeline_count(name), id}
    end
  end

  defp child_spec(%{pipeline: pipeline, name: name, pipeline_args: pipeline_args}, ref) do
    sharded_name = sharded_pipeline_name(name, ref)
    new_args = pipeline_args ++ [name: sharded_name]
    %{id: sharded_name, start: {pipeline, :start_link, [new_args]}}
  end

  def ack(_via, _successful, _failed) do
    :ok
  end

  def sharded_pipeline_name({:via, module, {registry, identifier}}, shard) do
    {:via, module, {registry, {__MODULE__, identifier, shard}}}
  end

  def pipeline_count(name) do
    Supervisor.which_children(name)
    |> Enum.filter(fn
      {_, _, _, [Agent]} -> false
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

  @doc """
  Get buffer lengths of each sharded pipeline
  """
  def buffer_len_by_pipeline(name) do
    for {id, _child, _type, _mod} <- Supervisor.which_children(name),
        id != Agent and id != Registry,
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
end
