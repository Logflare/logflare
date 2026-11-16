defmodule Logflare.Utils.TasksTest do
  use ExUnit.Case, async: true

  alias Logflare.Utils.Tasks

  describe "run_within/2" do
    test "returns the result when the function finishes in time" do
      assert Tasks.run_within(fn -> :done end, 1_000) == {:ok, :done}
    end

    test "kills the function and returns :timeout when it runs past the deadline" do
      test_pid = self()

      result =
        Tasks.run_within(
          fn ->
            send(test_pid, {:started, self()})
            Process.sleep(:infinity)
          end,
          100
        )

      assert result == :timeout
      assert_received {:started, pid}
      refute Process.alive?(pid)
    end

    @tag :capture_log
    test "returns :error when the function crashes, without crashing the caller" do
      assert Tasks.run_within(fn -> raise "boom" end, 1_000) == :error
    end
  end
end
