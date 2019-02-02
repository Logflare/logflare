defmodule Logflare.Application do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    # when we start populating tables on broot
    # we can pull all the table names from psql here
    # and set the state of Main

    children = [
      supervisor(Logflare.Repo, []),
      supervisor(LogflareWeb.Endpoint, []),
      supervisor(Logflare.Periodically, []),
      supervisor(Logflare.Main, []),
      supervisor(Logflare.Counter, []),
      {Task.Supervisor, name: Logflare.TaskSupervisor},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Logflare.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def config_change(changed, _new, removed) do
    LogflareWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
