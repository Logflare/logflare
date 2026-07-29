defmodule Logflare.Sources.IdleShutdownWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Logflare.Sources

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok
  def perform(_job) do
    Sources.shutdown_idle_sources()
  end
end
