defmodule LogflareTelemetry.Reporters.Ecto.Transformer.V0 do
  @moduledoc """
  Transforms Ecto telemetry events for further ingest
  """
  def prepare_metadata(telemetry_metadata) do
    telemetry_metadata
    |> Map.take([:params, :query, :source, :repo])
    |> Map.update!(:params, &inspect/1)
    |> Map.update!(:repo, &inspect/1)
  end

  def prepare_measurements(telemetry_measurements) do
    for {k, v} <- telemetry_measurements do
      {k, div(v, 1000)}
    end
    |> Map.new()
  end
end
