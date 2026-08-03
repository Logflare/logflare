defmodule LogflareWeb.UtcTimeLiveTest do
  use LogflareWeb.ConnCase, async: true

  alias LogflareWeb.UtcTimeLive

  test "tick re-renders the date with an updated time", %{conn: conn} do
    {:ok, view, _html} = live_isolated(conn, UtcTimeLive)

    initial_time = view |> render() |> extract_time()

    TestUtils.retry_assert([sleep: 1_000], fn ->
      updated_time = view |> render() |> extract_time()
      assert DateTime.before?(initial_time, updated_time)
    end)
  end

  defp extract_time(html) do
    [_, time_str] = Regex.run(~r/(\d{1,2}:\d{2}:\d{2}(?:am|pm)) UTC/, html)

    time_str
    |> Timex.parse!("{h12}:{m}:{s}{am}")
    |> DateTime.from_naive!("Etc/UTC")
  end
end
