defmodule Logflare.Application do
  use Application
  alias Logflare.Users

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      {Cachex, Users.Cache},
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Task.Supervisor, name: Logflare.TableSupervisor},
      supervisor(Logflare.Repo, []),
      supervisor(Logflare.AccountCache, []),
      # init TableCounter before TableManager as TableManager calls TableCounter through table create
      supervisor(Logflare.TableCounter, []),
      supervisor(Logflare.SystemCounter, []),
      supervisor(Logflare.TableManager, []),
      supervisor(LogflareWeb.Endpoint, [])
    ]

    children =
      if Mix.env() == :test do
        [
          {Cachex, Users.Cache},
          supervisor(Logflare.Repo, []),
          supervisor(LogflareWeb.Endpoint, [])
        ]
      else
        children
      end

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
