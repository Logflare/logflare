defmodule Logflare.QA.Ingest.Targets do
  @moduledoc """
  Routing targets for the ingest QA source.

  Each run sends one event of each kind over each channel. `expect` lists the kinds
  a target must receive and no others. Add a target here to route to more backends:
  setup and verification both read this list.
  """

  alias Logflare.QA.Ingest.Target

  @type t :: %{
          main: String.t(),
          channels: [String.t()],
          kinds: [String.t()],
          targets: [Target.t()]
        }

  @spec config() :: t()
  def config do
    %{
      main: "qa_ingest_main",
      channels: ["http_token", "http_name", "websocket", "grpc"],
      kinds: ["error", "warn", "info"],
      targets: [
        %Target{type: :source, name: "qa_ingest_sink", lql: "error", expect: ["error"]},
        %Target{type: :backend, name: "qa_ingest_drain_a", lql: "error", expect: ["error"]},
        %Target{type: :backend, name: "qa_ingest_drain_b", lql: "warn", expect: ["warn"]},
        %Target{
          type: :backend,
          name: "qa_ingest_drain_c",
          lql: ~s|~"error\|warn"|,
          expect: ["error", "warn"]
        }
      ]
    }
  end

  @spec message(String.t(), String.t(), String.t()) :: String.t()
  def message(kind, channel, run_id), do: "#{kind} from #{channel} #{run_id}"

  @doc "All messages a run sends over `channels`."
  @spec messages(t(), String.t(), [String.t()] | nil) :: [String.t()]
  def messages(config, run_id, channels \\ nil) do
    for channel <- channels || config.channels,
        kind <- config.kinds,
        do: message(kind, channel, run_id)
  end
end
