defmodule LogflareWeb.Router do
  use LogflareWeb, :router
  use PhoenixOauth2Provider.Router

  # TODO: move plug calls in SourceController and RuleController into here

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_flash)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
    plug(LogflareWeb.Plugs.SetUser)
  end

  pipeline :api do
    plug(:accepts, ["json"])
    plug(LogflareWeb.Plugs.SetApiUser)
  end

  pipeline :require_api_auth do
    plug(LogflareWeb.Plugs.VerifyApiRequest)
    plug(LogflareWeb.Plugs.CheckSourceCountApi)
  end

  pipeline :require_auth do
    plug(LogflareWeb.Plugs.RequireAuth)
  end

  pipeline :oauth_public do
    plug(:put_secure_browser_headers)
  end

  pipeline :check_admin do
    plug(LogflareWeb.Plugs.CheckAdmin)
  end

  scope "/" do
    pipe_through(:oauth_public)
    oauth_routes(:public)
  end

  scope "/" do
    pipe_through([:browser, :require_auth])
    oauth_routes(:protected)
  end

  scope "/", LogflareWeb do
    pipe_through(:browser)
    get("/", SourceController, :index)
  end

  scope "/", LogflareWeb do
    pipe_through([:browser, :require_auth])
    get("/dashboard", SourceController, :dashboard)
  end

  scope "/sources", LogflareWeb do
    pipe_through(:browser)
    get("/public/:public_token", SourceController, :public)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser, :require_auth])

    resources "/", SourceController, except: [:index] do
      post("/rules", RuleController, :create)
      get("/rules", RuleController, :index)
      delete("/rules/:id", RuleController, :delete)
    end
  end

  scope "/account", LogflareWeb do
    pipe_through([:browser, :require_auth])

    get("/edit", UserController, :edit)
    put("/edit", UserController, :update)
    delete("/", UserController, :delete)
  end

  scope "/admin", LogflareWeb do
    pipe_through([:browser, :check_admin])

    get("/dashboard", AdminController, :dashboard)
  end

  scope "/auth", LogflareWeb do
    pipe_through([:browser, :require_auth])
    get("/new-api-key", AuthController, :new_api_key)
  end

  scope "/auth", LogflareWeb do
    pipe_through(:browser)

    get("/login", AuthController, :login)
    get("/logout", AuthController, :logout)
    get("/:provider", AuthController, :request)
    get("/:provider/callback", AuthController, :callback)
  end

  scope "/api", LogflareWeb do
    pipe_through(:api)
    post("/cloudflare/event", CloudflareController, :event)
    post("/v1/cloudflare/event", CloudflareControllerV1, :event)
  end

  scope "/api", LogflareWeb do
    pipe_through([:api, :require_api_auth])
    post("/logs", LogController, :create)
  end
end
