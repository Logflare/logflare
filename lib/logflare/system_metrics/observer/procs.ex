defmodule Logflare.SystemMetrics.Observer.Procs do
  @moduledoc """
  Adapted from: https://github.com/sasa1977/demo_system/blob/master/example_system/lib/runtime.ex
  """

  @collect_procs_for 1_000

  def top(time \\ @collect_procs_for) do
    initial_processes = processes()

    # TODO make this into a genserver
    Process.sleep(time)

    final_processes =
      Enum.map(
        processes(),
        fn {name, reds} ->
          prev_reds = Map.get(initial_processes, name, 0)
          %{name: name, reds: reds - prev_reds}
        end
      )

    final_processes
    |> Enum.sort_by(& &1.reds, &>=/2)
    |> Stream.take(10)
    |> Enum.map(&%{name: &1.name, reds: &1.reds})
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
