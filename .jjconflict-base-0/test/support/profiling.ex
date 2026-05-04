# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
defmodule Logflare.Profiling do
  @moduledoc """
  Shared helpers for benchmark scripts under `test/profiling/`.

  Captures per-scenario stats from a `Benchee.Suite`, compares them against the
  most-recent entry in a history file, prints a delta table, and optionally
  appends a new entry when `SAVE_SNAPSHOT=1` is set.

  History files are read via `Code.eval_file/1` and written via `inspect/2` —
  the file is executed as Elixir code, not parsed as data, so hand-edits must
  remain valid Elixir terms.
  """

  @type stats :: %{
          ips: integer,
          wall_avg_us: float,
          wall_median_us: float,
          wall_p99_us: float,
          memory_avg_bytes: integer,
          reductions_avg: integer
        }

  @type entry :: %{meta: map, results: %{required(String.t()) => stats}}

  @spec track(Benchee.Suite.t(), Path.t()) :: :ok
  def track(%Benchee.Suite{} = suite, history_path) do
    results = capture_results(suite)
    history = load_history(history_path)

    new_entry = %{
      meta: %{
        captured_at: DateTime.utc_now() |> DateTime.to_iso8601(),
        git_sha: git_sha(),
        label: System.get_env("LABEL"),
        machine: System.get_env("MACHINE")
      },
      results: results
    }

    print_delta(results, history)

    if System.get_env("SAVE_SNAPSHOT") == "1" do
      save_snapshot(history_path, [new_entry | history])
    end

    :ok
  end

  @spec capture_results(Benchee.Suite.t()) :: %{required(String.t()) => stats}
  def capture_results(%Benchee.Suite{scenarios: scenarios}) do
    for s <- scenarios, into: %{} do
      stats = s.run_time_data.statistics
      mem = s.memory_usage_data.statistics
      reds = s.reductions_data.statistics
      name = result_name(s)

      {name,
       %{
         ips: round(stats.ips),
         wall_avg_us: Float.round(stats.average / 1_000, 2),
         wall_median_us: Float.round(stats.median / 1_000, 2),
         wall_p99_us: Float.round((stats.percentiles[99] || 0) / 1_000, 2),
         memory_avg_bytes: round(mem.average),
         reductions_avg: round(reds.average)
       }}
    end
  end

  defp result_name(%{name: name, input_name: :__no_input}), do: name
  defp result_name(%{name: name, input_name: input_name}), do: "#{name} / #{input_name}"

  @spec load_history(Path.t()) :: [entry]
  def load_history(path) do
    if File.exists?(path) do
      {history, _} = Code.eval_file(path)
      history
    else
      []
    end
  end

  @spec save_snapshot(Path.t(), [entry]) :: :ok
  def save_snapshot(path, history) do
    content =
      history
      |> inspect(pretty: true, limit: :infinity, custom_options: [sort_maps: true])
      |> underscore_integers()

    File.write!(path, content <> "\n")

    IO.puts("\nSaved snapshot to #{path}")
    :ok
  end

  # Adds `_` thousands separators to integer literals (5+ digits) in inspect/2
  # output. The lookbehind/lookahead anchor matches to pretty-printed map-value
  # context, so digit runs inside quoted strings (timestamps, SHAs) are left
  # alone.
  defp underscore_integers(text) do
    Regex.replace(~r/(?<=[: ])(-?\d{5,})(?=[,\n}\)\]])/, text, fn _, num ->
      underscore_digits(num)
    end)
  end

  defp underscore_digits("-" <> rest), do: "-" <> underscore_digits(rest)

  defp underscore_digits(digits) do
    digits
    |> String.reverse()
    |> String.replace(~r/(\d{3})(?=\d)/, "\\1_")
    |> String.reverse()
  end

  @spec print_delta(%{required(String.t()) => stats}, [entry]) :: :ok
  def print_delta(_results, []) do
    IO.puts("\nNo history yet — run with SAVE_SNAPSHOT=1 to seed.")
  end

  def print_delta(results, [latest | _]) do
    label = latest.meta[:label] || latest.meta.captured_at
    IO.puts("\nDelta vs #{latest.meta.git_sha} (#{label}):")

    for {fixture, new_stats} <- Enum.sort(results) do
      case latest.results[fixture] do
        nil ->
          IO.puts("  #{fixture}: no baseline in latest snapshot")

        old ->
          IO.puts("  #{fixture}")

          IO.puts(
            "    ips:  #{fmt_ips(old.ips)} → #{fmt_ips(new_stats.ips)}  (#{fmt_pct(old.ips, new_stats.ips)})"
          )

          IO.puts(
            "    wall: #{old.wall_median_us} → #{new_stats.wall_median_us} µs  (#{fmt_pct(old.wall_median_us, new_stats.wall_median_us)})"
          )

          IO.puts(
            "    mem:  #{fmt_bytes(old.memory_avg_bytes)} → #{fmt_bytes(new_stats.memory_avg_bytes)}  (#{fmt_pct(old.memory_avg_bytes, new_stats.memory_avg_bytes)})"
          )

          IO.puts(
            "    reds: #{fmt_count(old.reductions_avg)} → #{fmt_count(new_stats.reductions_avg)}  (#{fmt_pct(old.reductions_avg, new_stats.reductions_avg)})"
          )
      end
    end

    :ok
  end

  defp git_sha do
    case System.cmd("git", ["rev-parse", "--short=9", "HEAD"], stderr_to_stdout: true) do
      {sha, 0} -> String.trim(sha)
      _ -> "unknown"
    end
  end

  defp fmt_pct(0, 0), do: "0.0%"
  defp fmt_pct(0, _new), do: "+inf%"

  defp fmt_pct(old, new) do
    pct = Float.round((new - old) / old * 100, 1)
    if pct >= 0, do: "+#{pct}%", else: "#{pct}%"
  end

  defp fmt_bytes(b) when b >= 1_048_576, do: "#{Float.round(b / 1_048_576, 2)} MB"
  defp fmt_bytes(b), do: "#{Float.round(b / 1024, 1)} KB"

  defp fmt_count(n) when n >= 1000, do: "#{Float.round(n / 1000, 1)}K"
  defp fmt_count(n), do: "#{n}"

  defp fmt_ips(ips) when ips >= 1_000_000, do: "#{Float.round(ips / 1_000_000, 2)}M ips"
  defp fmt_ips(ips) when ips >= 1_000, do: "#{Float.round(ips / 1_000, 1)}K ips"
  defp fmt_ips(ips), do: "#{ips} ips"
end
