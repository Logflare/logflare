defmodule LogflareTelemetry.Aggregators.GenAggregator do

  def measurement_exists?(nil), do: false
  def measurement_exists?([]), do: false
  def measurement_exists?(_), do: true

end
