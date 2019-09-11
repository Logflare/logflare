defmodule LogflareTelemetry.LogflareMetrics do
  @moduledoc false
  defmodule All do
    @moduledoc false
    defstruct [:description, :event_name, :measurement, :name, :tag_values, :tags, :unit]
  end

  def all(event_name) when is_list(event_name) do
    measurement = :all
    name = event_name ++ [measurement]

    %All{
      description: nil,
      event_name: event_name,
      measurement: measurement,
      name: name,
      tag_values: [],
      tags: [],
      unit: :unit
    }
  end
end
