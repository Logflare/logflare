defmodule Logflare.Endpoints.FilesystemCacheClickHouseTest do
  use Logflare.DataCase

  alias Logflare.Endpoints

  @setting "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"

  test "ClickHouse receives the resolved setting after parameter conversion and row limiting" do
    insert(:plan)
    user = insert(:user)
    {_source, backend} = setup_clickhouse_test(user: user)
    start_supervised!({ClickHouseAdaptor, backend})

    endpoint =
      insert(:endpoint,
        user: user,
        backend: backend,
        language: :ch_sql,
        max_limit: 1,
        query:
          "SELECT @from AS start_time, @to AS end_time, toUInt8(getSetting('#{@setting}')) AS mode",
        filesystem_cache_policy: %{
          "start_param" => "from",
          "end_param" => "to",
          "timestamp_format" => "unix_seconds",
          "bypass_fill_above_seconds" => 3600
        }
      )

    assert {:ok, %{rows: [%{"mode" => 1, "start_time" => "0", "end_time" => "7200"}]}} =
             Endpoints.run_query(endpoint, %{"from" => "0", "to" => "7200"})

    assert {:ok, %{rows: [%{"mode" => 0}]}} =
             Endpoints.run_query(endpoint, %{"from" => "0", "to" => "3600"})
  end
end
