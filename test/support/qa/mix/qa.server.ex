defmodule Mix.Tasks.Qa.Server do
  @shortdoc "Runs the dev server in single-tenant Postgres mode for the QA tasks"
  @moduledoc """
  Runs `mix phx.server` in single-tenant Postgres mode as the distributed node that
  `mix qa.ingest` connects to. Creates the backend database, then creates and migrates
  the dev database.

      mix qa.server
  """
  use Mix.Task

  alias Logflare.QA.Config

  @impl Mix.Task
  def run(args) do
    create_database!(Config.backend_url())

    System.put_env(%{
      "LOGFLARE_SINGLE_TENANT" => "true",
      "POSTGRES_BACKEND_URL" => Config.backend_url(),
      "LOGFLARE_PUBLIC_ACCESS_TOKEN" => Config.public_token(),
      "GOOGLE_PROJECT_ID" => "logflare-qa",
      "PHX_HTTP_PORT" => to_string(URI.parse(Config.url()).port),
      "LOGFLARE_GRPC_PORT" => Config.grpc_port()
    })

    {_, 0} = System.cmd("epmd", ["-daemon"])
    {:ok, _} = Node.start(Config.node_name(), name_domain: :shortnames)
    Node.set_cookie(Config.cookie())

    Mix.Task.run("ecto.create", ["--quiet"])
    Mix.Task.run("ecto.migrate", ["--quiet"])
    Mix.Task.run("phx.server", args)
  end

  defp create_database!(url) do
    %URI{path: "/" <> database, userinfo: userinfo} = uri = URI.parse(url)
    [username, password] = String.split(userinfo, ":", parts: 2)
    {:ok, _} = Application.ensure_all_started(:postgrex)

    {:ok, conn} =
      Postgrex.start_link(
        hostname: uri.host,
        port: uri.port,
        username: username,
        password: password,
        database: "postgres"
      )

    case Postgrex.query(conn, ~s|CREATE DATABASE "#{database}"|, []) do
      {:ok, _} -> :ok
      {:error, %Postgrex.Error{postgres: %{code: :duplicate_database}}} -> :ok
    end

    GenServer.stop(conn)
  end
end
