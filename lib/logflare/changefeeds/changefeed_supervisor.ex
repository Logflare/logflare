defmodule Logflare.Changefeeds.SupervisorListener do
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
        {LocalRepo.ChangefeedListener,
         [%{notifications_pid: pid, changefeed: changefeed}, [name: :"#{changefeed}_listener"]]}
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
