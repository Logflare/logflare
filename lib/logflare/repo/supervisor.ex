defmodule Logflare.Repo.Supervisor do
  @moduledoc """
  Isolates `Logflare.Repo` and its read replicas from `Logflare.Supervisor`.

  `Ecto.Repo.Supervisor` supervises its connection pool with `max_restarts: 0`,
  so any pool exit terminates the repo immediately. Supervised directly by the
  application supervisor, four such exits within five seconds exhaust the default
  restart intensity and terminate the whole application.

  A brief outage does not restart the repo at all, so the intensity here is sized
  for the other case: a repo that cannot start. Supervisors restart immediately,
  with no backoff, so the budget is spent in milliseconds and the application
  terminates rather than looping silently - surfacing as a crash loop that pages
  someone instead of a node that is up but permanently unusable.
  """

  use Supervisor

  @max_restarts 10
  @max_seconds 300

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
