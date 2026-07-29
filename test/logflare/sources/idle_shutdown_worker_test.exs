defmodule Logflare.Sources.IdleShutdownWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Sources.IdleShutdownWorker

  test "perform/1 calls Sources.shutdown_idle_sources/0 and returns :ok" do
    assert :ok = perform_job(IdleShutdownWorker, %{})
  end
end
