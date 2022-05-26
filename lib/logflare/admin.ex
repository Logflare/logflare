defmodule Logflare.Admin do
  @moduledoc false
  require Logger
  @doc """
  Shuts down a given node asyncronously in a separate process.

  A delay (default is 5s) occurs just before systen stop is triggered.
  """
  @spec shutdown(node(), integer()) :: {:ok, %Task{}}
  def shutdown(node \\ Node.self(), delay \\ 5000) when is_atom(node) do
    task = Task.async(fn ->
      Logger.warn("Node shutdown initialized, shutting down in #{delay}ms. node=#{node}")
      Process.sleep(delay)

      :rpc.eval_everywhere([node], System, :stop, [])
    end)
    {:ok, task}
  end
end
