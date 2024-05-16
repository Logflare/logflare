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
    pipeline = Keyword.get(args, :pipeline)
    pipeline_args = Keyword.get(args, :pipeline_args, [])

    children = [
      {Agent, fn -> args end},
      child_spec(args, 0)
    ]

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
    args = get_state(name)
    pipeline = args[:pipeline]

    for {id, child, type, mod} <- Supervisor.which_children(name) |> hd() do
      Broadway.push_message(id, messages)
    end
  end

  def add_shard(name) do
    count = shard_count(name)
    args = get_state(name)
    spec = child_spec(args, count + 1)

    case Supervisor.start_child(name, spec) do
      {:ok, _pid} ->
        {:ok, shard_count(name)}

      err ->
        err
    end
  end

  defp child_spec(args, shard_num) do
    pipeline = Keyword.get(args, :pipeline)
    name = Keyword.get(args, :name)
    pipeline_args = Keyword.get(args, :pipeline_args, [])
    sharded_name = sharded_pipeline_name(name, shard_num)
    new_args = pipeline_args ++ [name: sharded_name]
    %{id: sharded_name, start: {pipeline, :start_link, [new_args]}}
  end

  def ack(_via, _successful, _failed) do
    :ok
  end

  def sharded_pipeline_name({:via, module, {registry, {source_id, {mod, backend_id}}}}, shard) do
    {:via, module, {registry, {source_id, {mod, backend_id, shard}}}}
  end

  def shard_count(name) do
    Supervisor.which_children(name)
    |> Enum.filter(fn
      {_, _, _, [Agent]} -> false
      _ -> true
    end)
    |> length()
  end

  def buffer_len(name) do
    for {id, child, type, mod} <- Supervisor.which_children(name),
        id != Agent,
        name <- Broadway.producer_names(id) do
      dbg(name)

      Task.async(fn ->
        GenStage.estimate_buffered_count(name)
      end)
    end
    |> Task.await_many()
    |> dbg()
    |> Enum.sum()
  end
end
