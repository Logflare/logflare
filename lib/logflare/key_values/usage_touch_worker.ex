defmodule Logflare.KeyValues.UsageTouchWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok
  def perform(_job) do
    Cache.touch_recent_usages()
    KeyValues.prune_usages()
    :ok
  end
end
