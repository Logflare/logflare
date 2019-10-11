defmodule LogflareTelemetry.ExtendedMetrics do
  @moduledoc false
  defmodule Every do
    @moduledoc false
    defstruct [:description, :event_name, :measurement, :name, :tag_values, :tags, :unit]
  end

  defmodule LastValues do
    @moduledoc false
    defstruct [:description, :event_name, :measurement, :name, :tag_values, :tags, :unit]
  end

  def every(event_name) when is_list(event_name) do
    measurement = :every
    name = event_name

    %Every{
      description: nil,
      event_name: event_name,
      measurement: measurement,
      name: name,
      tag_values: [],
      tags: [],
      unit: :unit
    }
  end

  def last_values(event_name) when is_list(event_name) do
    measurements = & &1
    name = event_name

    %LastValues{
      description: nil,
      event_name: event_name,
      measurement: measurements,
      name: name,
      tag_values: [],
      tags: [],
      unit: :unit
    }
  end
end
