defmodule Logflare.Utils.Tasks do
  @moduledoc """
  Utility functions for spawning supervised tasks with `Logflare.TaskSupervisors`

  https://hexdocs.pm/elixir/1.14/Task.Supervisor.html

  """

  @doc """
  Linked to caller, linked to supervisor
  """
  def async(mod, fun, args, opts \\ []) do
    Task.Supervisor.async(
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
      mod,
      fun,
      args,
      opts
    )
  end

  @doc """
  Linked to caller, linked to supervisor
  """
  @spec async((-> any())) :: Task.t()
  def async(func, opts \\ []) do
    Task.Supervisor.async(
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
      func,
      opts
    )
  end

  @doc """
  Not linked to caller, only to supervisor.
  """
  def start_child(func, opts \\ []) do
    Task.Supervisor.start_child(
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
      func,
      opts
    )
  end

  @doc """
  Kills all tasks under the supervisor.
  Used for test teardown, to prevent ecto sandbox checkout errors.
  """
  def kill_all_tasks do
    Logflare.TaskSupervisors
    |> PartitionSupervisor.which_children()
    |> Enum.map(fn {_, pid, _, _} ->
      pid |> Task.Supervisor.children() |> Enum.map(&Task.Supervisor.terminate_child(pid, &1))
    end)
  end
end
