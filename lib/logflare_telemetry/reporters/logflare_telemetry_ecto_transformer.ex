defmodule LogflareTelemetry.Reporters.Ecto.Transformer.V0 do
  def prepare_metadata(telemetry_metadata) do
    telemetry_metadata
    |> Map.take([:params, :query, :source, :repo])
    |> Map.update!(:params, &inspect/1)
    |> Map.update!(:repo, &inspect/1)
  end
end
