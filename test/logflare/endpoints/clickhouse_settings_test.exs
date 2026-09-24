defmodule Logflare.Endpoints.ClickHouseSettingsTest do
  use ExUnit.Case, async: true

  alias Logflare.Endpoints.ClickHouseSettings

  test "normalizes positive limits and forces read overflow to throw" do
    assert {:ok, %{"max_bytes_to_read" => 100, "read_overflow_mode" => "throw"}} =
             ClickHouseSettings.normalize(%{"max_bytes_to_read" => 100})

    for settings <- [
          %{"max_bytes_to_read" => 0},
          %{"max_memory_usage" => "100"},
          %{"read_overflow_mode" => "break"},
          %{"allow_introspection_functions" => 1},
          %{"max_execution_time" => -1}
        ] do
      assert {:error, _} = ClickHouseSettings.normalize(settings)
    end
  end

  test "preserves caller settings and adds resource limits to the final query" do
    query = "WITH src AS (SELECT a FROM t) SELECT a FROM src SETTINGS optimize_read_in_order = 1"
    settings = %{"max_execution_time" => 5, "max_memory_usage" => 100}

    assert {:ok, transformed} = ClickHouseSettings.enforce(query, settings)
    assert transformed =~ "WITH src AS (SELECT a FROM t)"
    assert transformed =~ "optimize_read_in_order = 1"
    assert transformed =~ "max_execution_time = 5"
    assert transformed =~ "max_memory_usage = 100"
  end

  test "rejects conflicting settings in the outer query or a nested query" do
    settings = %{"max_execution_time" => 5}

    for query <- [
          "SELECT a FROM t SETTINGS max_execution_time = 10",
          "WITH src AS (SELECT a FROM t SETTINGS max_execution_time = 10) SELECT a FROM src"
        ] do
      assert {:error, "ClickHouse setting max_execution_time is enforced by this endpoint"} =
               ClickHouseSettings.enforce(query, settings)
    end
  end
end
