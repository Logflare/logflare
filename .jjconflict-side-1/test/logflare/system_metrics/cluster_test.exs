defmodule Logflare.SystemMetrics.ClusterTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  alias Logflare.SystemMetrics.Cluster

  describe "finch/0" do
    test "does not raise when pool is not available" do
      capture_log(fn ->
        GenServer.stop(Logflare.FinchDefault)
        GenServer.stop(Logflare.FinchIngest)
        GenServer.stop(Logflare.FinchQuery)
        assert Cluster.finch()

        Process.sleep(100)
      end) =~ ":gen_statem"
    end
  end
end
