defmodule LogflareWeb.Router do
  use LogflareWeb, :router
  use PhoenixOauth2Provider.Router

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
    plug(LogflareWeb.Plugs.VerifyApiRequest)
    plug(LogflareWeb.Plugs.CheckSourceCountApi)
  end

  pipeline :oauth_public do
    plug(:put_secure_browser_headers)
  end

  scope "/" do
    pipe_through(:oauth_public)
    oauth_routes(:public)
  end

  scope "/" do
    pipe_through(:browser)
    oauth_routes(:protected)
  end

  scope "/", LogflareWeb do
    # Use the default browser stack
    pipe_through(:browser)
    get("/", SourceController, :index)
    get("/dashboard", SourceController, :dashboard)
  end

  scope "/sources", LogflareWeb do
    pipe_through(:browser)

    # get "/new", SourceController, :new
    # post "/", SourceController, :create
    # get "/:id", SourceController, :show
    # delete "/:id", SourceController, :delete
    # edit "/:id/edit", SourceController, :edit
    # put "/:id", SourceController, :update
    get("/:id/public/:public_token", SourceController, :public)

    resources "/", SourceController, except: [:index] do
      post("/rules", RuleController, :create)
      get("/rules", RuleController, :index)
      delete("/rules/:id", RuleController, :delete)
    end
  end

  scope "/auth", LogflareWeb do
    pipe_through(:browser)

    get("/logout", AuthController, :logout)
    get("/new-api-key", AuthController, :new_api_key)
    get("/:provider", AuthController, :request)
    get("/:provider/callback", AuthController, :callback)
  end

  scope "/api", LogflareWeb do
    pipe_through(:api)
    post("/logs", LogController, :create)
  end
end
