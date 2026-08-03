defmodule Logflare.GenSingleton do
  @moduledoc """
  A generic singleton GenServer that will be unique cluster-wide, started under the supervision tree.

  Creates a monitoring process that will start the corresponding child spec under the parent supervisor.

  If Watcher terminates abnormally, parent supervisor will restart all processes.
  """
  use Supervisor

  alias Logflare.GenSingleton
  alias Logflare.GenSingleton.Watcher

  @doc """
  Start the supervisor.
  """
  @spec start_link(Watcher.options()) :: Supervisor.on_start()
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, [])
  end

  @impl Supervisor
  def init(args) do
    children = [
      {Watcher, args}
    ]

    # reset state
    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Get the pid of the local process.
  """
  @spec get_pid(pid()) :: pid() | nil
  def get_pid(sup_pid) do
    Supervisor.which_children(sup_pid)
    |> Enum.find_value(fn
      {_id, pid, _type, [Watcher]} -> pid
      _ -> false
    end)
    |> GenSingleton.Watcher.get_pid()
  end

  @doc """
  Stop the local process.
  Noop if supervisor is not found. Noop if the process is not found on the supervisor.
  """
  @spec stop_local(pid()) :: :ok
  def stop_local(sup_pid) do
    if Process.alive?(sup_pid) do
      Supervisor.which_children(sup_pid)
      |> Enum.find_value(fn
        {_id, _pid, _type, [Watcher]} -> false
        {id, pid, _type, _modules} -> {id, pid}
        _ -> false
      end)
      |> case do
        nil -> :ok
        {id, _pid} -> Supervisor.terminate_child(sup_pid, id)
      end
    end
  end
end
