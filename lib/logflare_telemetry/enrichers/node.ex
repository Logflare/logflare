defmodule LogflareTelemetry.Enricher do
  def beam_context() do
    %{
      "current_node" => current_node()
    }
  end

  def current_node() do
    "#{Node.self()}"
  end
end
