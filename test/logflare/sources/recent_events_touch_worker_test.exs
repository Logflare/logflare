defmodule Logflare.Sources.RecentEventsTouchWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Sources.RecentEventsTouchWorker

  test "perform/1 calls Sources.recent_events_touch/0 and returns :ok" do
    assert :ok = perform_job(RecentEventsTouchWorker, %{})
  end
end
