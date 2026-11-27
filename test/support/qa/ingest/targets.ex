defmodule Logflare.QA.Ingest.Targets do
  @moduledoc """
  The ingest QA source and its routing targets. Each run sends one event of each kind
  over each channel, and a target must store exactly the kinds it lists. Add a target
  here to route to another backend.
  """

  @channels ["http_token", "http_name", "websocket", "grpc"]
  @kinds ["error", "warn", "info"]

  @type target ::
          {:source | :backend, name :: String.t(), lql :: String.t(), kinds :: [String.t()]}

  @spec main() :: String.t()
  def main, do: "qa_ingest_main"

  @spec targets() :: [target()]
  def targets do
    [
      {:source, "qa_ingest_sink", "error", ["error"]},
      {:backend, "qa_ingest_drain_a", "error", ["error"]},
      {:backend, "qa_ingest_drain_b", "warn", ["warn"]},
      {:backend, "qa_ingest_drain_c", ~s|~"error\|warn"|, ["error", "warn"]}
    ]
  end

  @spec kinds() :: [String.t()]
  def kinds, do: @kinds

  @doc "Messages a run sends over every channel, limited to `kinds`."
  @spec messages(String.t(), [String.t()]) :: [String.t()]
  def messages(run_id, kinds \\ @kinds),
    do: for(channel <- @channels, message <- batch(channel, run_id, kinds), do: message)

  @doc "The batch a run sends over one channel."
  @spec batch(String.t(), String.t(), [String.t()]) :: [String.t()]
  def batch(channel, run_id, kinds \\ @kinds),
    do: for(kind <- kinds, do: "#{kind} from #{channel} #{run_id}")
end
