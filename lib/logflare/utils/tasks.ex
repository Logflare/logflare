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
  Linked to caller, linked to supervisor.

  In test environment, this function automatically handles database ownership
  for spawned tasks to prevent Ecto.Sandbox errors.
  """
  @spec async((-> any())) :: Task.t()
  def async(func, opts \\ []) do
    wrapped_func = maybe_wrap_for_test_db(func)

    Task.Supervisor.async(
      {:via, PartitionSupervisor, {Logflare.TaskSupervisors, self()}},
      wrapped_func,
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

  defp maybe_wrap_for_test_db(func) do
    if Application.get_env(:logflare, :env) == :test do
      owner_pid = self()

      fn ->
        setup_test_db_ownership(owner_pid)
        func.()
      end
    else
      func
    end
  end

  # Ensure tests have database access for tasks spawned through Task.Supervisor
  defp setup_test_db_ownership(owner_pid) do
    try do
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo)
    rescue
      DBConnection.OwnershipError ->
        # If checkout fails, try to allow from owner
        try do
          Ecto.Adapters.SQL.Sandbox.allow(Logflare.Repo, owner_pid, self())
        rescue
          _ -> :ok
        end
    end
  end
end
