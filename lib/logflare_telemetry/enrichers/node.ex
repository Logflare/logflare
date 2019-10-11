defmodule LogflareTelemetry.Enricher do
  @moduledoc """
  Adds enriched information about the BEAM and OTP to the telemetry event payloads
  """

  def beam_context() do
    %{
      "node" => current_node()
    }
  end

  def current_node() do
    "#{Node.self()}"
  end
end
