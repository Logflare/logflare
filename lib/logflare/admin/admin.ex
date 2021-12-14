defmodule Logflare.Admin do
  @moduledoc """
    A place for random admin functions.
  """

  def shutdown() do
    Task.async(fn ->
      Process.sleep(5_000)

      System.stop()
    end)
  end

  def shutdown(node) when is_atom(node) do
    Task.async(fn ->
      Process.sleep(5_000)

      :rpc.eval_everywhere([node], System, :stop, [])
    end)
  end
end
