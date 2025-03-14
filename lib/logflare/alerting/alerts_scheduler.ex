defmodule Logflare.Alerting.AlertsScheduler do
  use Quantum, otp_app: :logflare, restart: :transient
  alias Logflare.Alerting
  require Logger
  @impl Quantum
  def init(config) do
    jobs = Alerting.init_alert_jobs()
    Keyword.put(config, :jobs, jobs)
  end
end
