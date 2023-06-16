defmodule LogflareWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :logflare
  @session_options [store: :cookie, key: "_logflare_key", signing_salt: "INPMyhPE"]

  socket("/socket", LogflareWeb.UserSocket, websocket: [compress: true])

  socket("/logs", LogflareWeb.LogSocket, websocket: [compress: true])

  socket("/live", Logflare.LiveView.Socket,
    websocket: [connect_info: [session: @session_options], compress: true]
  )

  # Serve at "/" the static files from "priv/static" directory.
  #
  # You should set gzip to true if you are running phoenix.digest
  # when deploying your static files in production.
  plug(Plug.Static,
    at: "/",
    from: :logflare,
    gzip: !code_reloading?,
    only: ~w(css fonts images js favicon.ico robots.txt worker.js manifest.json),
    only_matching: ~w(manifest)
  )

  # Code reloading can be explicitly enabled under the
  # :code_reloader configuration of your endpoint.
  if code_reloading? do
    socket("/phoenix/live_reload/socket", Phoenix.LiveReloader.Socket)
    plug(Phoenix.LiveReloader)
    plug(Phoenix.CodeReloader)
  end

  plug(Plug.Logger, log: :debug)

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Jason
  )

  plug(Plug.MethodOverride)
  plug(Plug.Head)

  # The session will be stored in the cookie and signed,
  # this means its contents can be read but not tampered with.
  # Set :encryption_salt if you would also like to encrypt it.
  plug(Plug.Session, @session_options)

  plug(LogflareWeb.Router)

  @doc """
  Callback invoked for dynamically configuring the endpoint.

  It receives the endpoint configuration and checks if
  configuration should be loaded from the system environment.
  """
  def init(_key, config) do
    if config[:load_from_system_env] do
      port = System.get_env("PORT") || raise "expected the PORT environment variable to be set"

      config =
        config
        |> Keyword.put(:http, [:inet6, port: port])
        |> Keyword.put(:live_dashboard, Application.get_env(:logflare, :live_dashboard))

      {:ok, config}
    else
      {:ok, config}
    end
  end
end
