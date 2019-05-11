defmodule Logflare.Application do
  use Application
  alias Logflare.{Users, Sources}

  def start(_type, _args) do
    import Supervisor.Spec

    children = [
      %{
        id: :cachex_users_cache,
        start: {Cachex, :start_link, [Users.Cache]}
      },
      %{
        id: :cachex_sources_cache,
        start: {Cachex, :start_link, [Sources.Cache]}
      },
      supervisor(Logflare.Repo, []),
      supervisor(LogflareWeb.Endpoint, [])
    ]

    dev_prod_children = [
      {Task.Supervisor, name: Logflare.TaskSupervisor},
      {Task.Supervisor, name: Logflare.TableSupervisor},
      supervisor(Logflare.AccountCache, []),
      # init TableCounter before TableManager as TableManager calls TableCounter through table create
      supervisor(Logflare.TableCounter, []),
      supervisor(Logflare.SystemCounter, []),
      supervisor(Logflare.TableManager, [])
    ]

    children =
      if Mix.env() == :test do
        children
      else
        children ++ dev_prod_children
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
