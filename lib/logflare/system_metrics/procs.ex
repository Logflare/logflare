defmodule Logflare.SystemMetrics.Procs do
  @moduledoc """
  Adapted from: https://github.com/sasa1977/demo_system/blob/master/example_system/lib/runtime.ex
  """

  @collect_procs_for 1_000

  def top(time \\ @collect_procs_for) do
    #  wall_times = LoadControl.SchedulerMonitor.wall_times()
    initial_processes = processes()

    Process.sleep(time)

    final_processes =
      Enum.map(
        processes(),
        fn {name, reds} ->
          prev_reds = Map.get(initial_processes, name, 0)
          %{name: name, reds: reds - prev_reds}
        end
      )

    #    schedulers_usage =
    #      LoadControl.SchedulerMonitor.usage(wall_times) / :erlang.system_info(:schedulers_online)

    # total_reds_delta = final_processes |> Stream.map(& &1.reds) |> Enum.sum()

    final_processes
    |> Enum.sort_by(& &1.reds, &>=/2)
    |> Stream.take(10)
    |> Enum.map(&%{name: &1.name, reds: &1.reds})

    # |> Enum.map(&%{pid: &1.pid, cpu: round(schedulers_usage * 100 * &1.reds / total_reds_delta)})
  end

  defp processes() do
    for {{:registered_name, name}, {:reductions, reds}} <-
          Stream.map(
            Process.list(),
            &{Process.info(&1, :registered_name), Process.info(&1, :reductions)}
          ),
        into: %{},
        do: {name, reds}
  end
end
