defmodule Logflare.GenSingleton do
  @moduledoc """
  A generic singleton GenServer that will be unique cluster-wide, started under the supervision tree.
  """

  use Supervisor
  alias Logflare.GenSingleton

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, [])
  end

  def init(args) do
    children = [
      {GenSingleton.Watcher, args}
    ]

    # reset state
    Supervisor.init(children, strategy: :rest_for_one)
  end

  def get_pid(sup_pid) do
    Supervisor.which_children(sup_pid)
    |> Enum.find_value(fn
      {_id, pid, _type, [GenSingleton.Watcher]} -> pid
      _ -> false
    end)
    |> GenSingleton.Watcher.get_pid()
  end
end
