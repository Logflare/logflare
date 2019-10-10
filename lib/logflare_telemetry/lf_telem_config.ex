defmodule LogflareTelemetry.Config do
  @moduledoc false
  defstruct [:tick_interval, :metrics, :backend, :beam, :ecto, :redix]
end
