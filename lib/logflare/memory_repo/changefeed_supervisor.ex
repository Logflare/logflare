defmodule Logflare.MemoryRepo.ChangefeedsSupervisor do
  use Logflare.Commons

  use Supervisor

  require Logger

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args[:changefeeds], name: __MODULE__)
  end

  @impl true
  def init(changefeeds) do
    {:ok, pid} = Postgrex.Notifications.start_link(Repo.config())

    children =
      for changefeed <- changefeeds do
        {MemoryRepo.ChangefeedListener, [%{notifications_pid: pid, changefeed: changefeed}]}
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
