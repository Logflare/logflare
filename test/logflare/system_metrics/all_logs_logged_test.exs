defmodule Logflare.SystemMetrics.AllLogsLoggedTest do
  use ExUnit.Case, async: false

  alias Logflare.SystemMetrics.AllLogsLogged

  test "preserves the initial count and reports increments" do
    assert {:ok, :total_logs_logged} = AllLogsLogged.create(:total_logs_logged, 41)
    assert :ets.info(:system_counter, :write_concurrency) == :auto
    assert {:ok, 41} = AllLogsLogged.log_count(:total_logs_logged)
    assert {:ok, 41} = AllLogsLogged.init_log_count(:total_logs_logged)

    assert {:ok, :total_logs_logged} = AllLogsLogged.increment(:total_logs_logged, 5)
    assert {:ok, 46} = AllLogsLogged.log_count(:total_logs_logged)

    assert {:ok, %{inserts_since_init: 5, init_log_count: 41, total: 46}} =
             AllLogsLogged.all_metrics(:total_logs_logged)
  end

  test "increments the total concurrently without losing updates" do
    workers = 16
    increments_per_worker = 1_000

    assert {:ok, :total_logs_logged} = AllLogsLogged.create(:total_logs_logged)

    1..workers
    |> Task.async_stream(
      fn _worker ->
        for _increment <- 1..increments_per_worker do
          assert {:ok, :total_logs_logged} = AllLogsLogged.increment(:total_logs_logged)
        end

        :ok
      end,
      max_concurrency: workers,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.each(fn result -> assert result == {:ok, :ok} end)

    assert AllLogsLogged.log_count(:total_logs_logged) ==
             {:ok, workers * increments_per_worker}
  end
end
