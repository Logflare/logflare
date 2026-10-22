defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettingsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryClassSettings
  alias Logflare.Endpoints.ClickHouseSettings

  @classes %{
    "default" => %{"priority" => 5, "max_threads" => 16},
    "api_paid" => %{"priority" => 10, "max_threads" => 4},
    "mcp" => %{"priority" => 10},
    "dashboard_logs_paid" => %{"priority" => 1}
  }

  test "selects the requested class and defaults missing or unknown labels" do
    for label <- [nil, "unknown", "api_free"] do
      assert {:ok, %{"priority" => 5, "max_threads" => 16}} =
               QueryClassSettings.resolve(@classes, label, %{})
    end

    assert {:ok, %{"priority" => 10, "max_threads" => 4}} =
             QueryClassSettings.resolve(@classes, "api_paid", %{})

    assert {:ok, %{"priority" => 1, "max_threads" => 16}} =
             QueryClassSettings.resolve(@classes, "dashboard_logs_paid", %{})

    assert {:ok, %{}} = QueryClassSettings.resolve(%{}, "api_paid", %{})
  end

  test "limits use stricter-wins across defaults, class and endpoint policy" do
    classes = %{
      "default" => %{"priority" => 5, "max_threads" => 4, "max_execution_time" => 65},
      "api_paid" => %{"priority" => 10, "max_threads" => 16, "max_execution_time" => 30}
    }

    assert {:ok, settings} =
             QueryClassSettings.resolve(classes, "api_paid", %{
               "max_threads" => 8,
               "max_execution_time" => 0.5,
               "max_memory_usage" => 1024
             })

    assert settings["priority"] == 10
    assert settings["max_threads"] == 4
    assert settings["max_execution_time"] == 0.5
    assert settings["max_memory_usage"] == 1024
    assert settings["timeout_overflow_mode"] == "throw"
  end

  test "rejects unknown classes, missing defaults, wrong types and unsafe settings" do
    for config <- [
          nil,
          %{"api_paid" => %{"priority" => 10}},
          %{"default" => %{}},
          %{"default" => %{"priority" => 0}},
          %{"default" => %{"priority" => 1.5}},
          %{"default" => %{"priority" => 5}, "api_unknown" => %{}},
          %{"default" => %{"priority" => 5}, "api_paid" => %{"max_threads" => "4"}},
          %{"default" => %{"priority" => 5}, "api_paid" => %{"max_threads" => 1.5}},
          %{"default" => %{"priority" => 5, "readonly" => 0}},
          %{"default" => %{"priority" => 5, "join_use_nulls" => 1}},
          %{"default" => %{"priority" => 5, "read_overflow_mode" => "break"}},
          %{"default" => %{"priority" => 5, "timeout_overflow_mode" => "break"}}
        ] do
      assert {:error, _} = QueryClassSettings.normalize(config)
    end
  end

  test "serializes fractional limits and fixed enums safely" do
    assert {:ok, settings} =
             ClickHouseSettings.normalize(%{
               "max_execution_time" => 0.5,
               "max_rows_to_read" => 100
             })

    assert settings["read_overflow_mode"] == "throw"
    assert settings["timeout_overflow_mode"] == "throw"
    assert {:ok, sql} = ClickHouseSettings.enforce("SELECT a FROM t", settings)
    assert sql =~ "max_execution_time = 0.5"
    assert sql =~ "read_overflow_mode = 'throw'"
    assert sql =~ "timeout_overflow_mode = 'throw'"

    assert {:error, _} =
             ClickHouseSettings.normalize(%{"read_overflow_mode" => "throw'; SELECT 1"})
  end

  test "rejects class overrides inside SELECT and EXPLAIN nested queries" do
    assert {:ok, policy} = QueryClassSettings.resolve(@classes, "api_paid", %{})

    for sql <- [
          "SELECT a FROM t SETTINGS priority = 0",
          "WITH t AS (SELECT a FROM s SETTINGS max_threads = 32) SELECT a FROM t",
          "EXPLAIN SELECT a FROM t SETTINGS priority = 1"
        ] do
      assert {:error, reason} = ClickHouseSettings.enforce(sql, policy)
      assert reason =~ "is enforced"
    end
  end
end
