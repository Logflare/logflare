defmodule Logflare.Utils.Tasks do
  @moduledoc """
  Utility functions for spawning supervised tasks with `Logflare.TaskSupervisor`

  https://hexdocs.pm/elixir/1.14/Task.Supervisor.html

  """

  @doc """
  Linked to caller, linked to supervisor
  """
  def async(func, opts \\ []) do
    Task.Supervisor.async(Logflare.TaskSupervisor, func, opts)
  end


  @doc """
  Not linked to caller, only to supervisor.
  """
  def start_child(func, opts \\ []) do
    Task.Supervisor.start_child(Logflare.TaskSupervisor, func, opts)
  end


  @doc """
  Kills all tasks under the supervisor.
  Used for test teardown, to prevent ecto sandbox checkout errors.
  """
  def kill_all_tasks do
    Logflare.TaskSupervisor
    |> Task.Supervisor.children()
    |> Enum.map(&Task.Supervisor.terminate_child(Logflare.TaskSupervisor, &1))
  end
end
