defmodule Logflare.Repo.Supervisor do
  @moduledoc """
  Isolates `Logflare.Repo` and its read replicas from `Logflare.Supervisor`.

  `Ecto.Repo.Supervisor` supervises its connection pool with `max_restarts: 0`,
  so any pool exit terminates the repo immediately. Supervised directly by the
  application supervisor, four such exits within five seconds exhaust the default
  restart intensity and terminate the whole application.

  The high intensity over a one second period means the budget resets constantly,
  so reconnect churn during a database outage never trips it while a repo that
  fails on every start still does.
  """

  use Supervisor

  @max_restarts 1_000
  @max_seconds 1

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(_opts) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl Supervisor
  def init(:ok) do
    read_replicas = Application.get_env(:logflare, :read_replicas, [])

    children = [
      Logflare.Repo,
      {Logflare.Repo.Replicas, entries: read_replicas}
    ]

    Supervisor.init(children,
      strategy: :one_for_one,
      max_restarts: @max_restarts,
      max_seconds: @max_seconds
    )
  end
end
