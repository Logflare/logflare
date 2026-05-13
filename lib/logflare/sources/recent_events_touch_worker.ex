defmodule Logflare.Sources.RecentEventsTouchWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Logflare.Sources

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok
  def perform(_job) do
    Sources.recent_events_touch()
    :ok
  end
end
