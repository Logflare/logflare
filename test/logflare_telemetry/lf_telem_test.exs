defmodule LogflareTelemetry.MainTest do
  @moduledoc false
  use ExUnit.Case
  use Placebo
  alias LogflareTelemetry.Aggregators.GenAggregator
  alias Logflare.TelemetryBackend.BQ, as: Backend

  setup do
    LogflareTelemetry.Supervisor.start_link()
    :ok
  end

  describe "main test" do
    test "broadcast state" do
      expect Backend.ingest([
               [
                 %{
                   "message" => "vm",
                   "metadata" => %{
                     "vm" => %{
                       "memory" => %{
                         "last_values" => %{
                           "atom" => any(),
                           "atomany()used" => any(),
                           "binary" => any(),
                           "code" => any(),
                           "ets" => any(),
                           "processes" => any(),
                           "processesany()used" => any(),
                           "system" => any(),
                           "total" => any()
                         }
                       }
                     }
                   }
                 }
               ]
             ]),
             return: :ok

      Process.sleep(5_000)
    end
  end
end
