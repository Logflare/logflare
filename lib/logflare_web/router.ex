defmodule LogflareWeb.Router do
  use LogflareWeb, :router
  use PhoenixOauth2Provider.Router, otp_app: :logflare

  # TODO: move plug calls in SourceController and RuleController into here

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_flash
    plug Phoenix.LiveView.Flash
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug LogflareWeb.Plugs.SetVerifyUser
  end

  pipeline :api do
    plug Plug.Parsers,
      parsers: [:json, :bert],
      json_decoder: Jason

    plug :accepts, ["json", "bert"]
  end

  pipeline :require_api_auth do
    plug LogflareWeb.Plugs.SetVerifyUser
    plug LogflareWeb.Plugs.SetVerifySource
    plug LogflareWeb.Plugs.RateLimiter
  end

  pipeline :require_auth do
    plug LogflareWeb.Plugs.RequireAuth
  end

  pipeline :oauth_public do
    plug :accepts, ["json"]
    plug :put_secure_browser_headers
  end

  pipeline :check_admin do
    plug LogflareWeb.Plugs.CheckAdmin
  end

  scope "/" do
    pipe_through [:api, :oauth_public]
    oauth_api_routes()
  end

  scope "/" do
    pipe_through [:browser, :require_auth]
    oauth_routes()
  end

  scope "/", LogflareWeb do
    pipe_through :browser
    get "/", MarketingController, :index
    get "/terms", MarketingController, :terms
    get "/privacy", MarketingController, :privacy
    get "/cookies", MarketingController, :cookies
    get "/contact", ContactController, :contact
    post "/contact", ContactController, :new
    get "/guides", MarketingController, :guides
  end

  scope "/guides", LogflareWeb do
    pipe_through :browser
    get "/overview", MarketingController, :overview
    get "/bigquery-setup", MarketingController, :big_query_setup
    get "/data-studio-setup", MarketingController, :data_studio_setup
    get "/event-analytics", MarketingController, :event_analytics_demo
    get "/log-search", MarketingController, :log_search
    get "/getting-started", MarketingController, :getting_started
  end

  scope "/", LogflareWeb do
    pipe_through [:browser, :require_auth]
    get "/dashboard", SourceController, :dashboard
  end

  scope "/sources", LogflareWeb do
    pipe_through :browser
    get "/public/:public_token", SourceController, :public
    get "/:id/unsubscribe/:token", AuthController, :unsubscribe
    get "/:id/unsubscribe/stranger/:token", AuthController, :unsubscribe_stranger
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :require_auth]

    resources "/", SourceController, except: [:index] do
      post "/rules", RuleController, :create
      get "/rules", RuleController, :index
      delete "/rules/:id", RuleController, :delete
    end

    get "/:id/rejected", SourceController, :rejected_logs
    get "/:id/search", SourceController, :search

    get "/:id/favorite", SourceController, :favorite
    get "/:id/clear", SourceController, :clear_logs
    get "/:id/explore", SourceController, :explore
  end

  scope "/account", LogflareWeb do
    pipe_through [:browser, :require_auth]

    get "/edit", UserController, :edit
    put "/edit", UserController, :update
    delete "/", UserController, :delete
  end

  scope "/admin", LogflareWeb do
    pipe_through [:browser, :check_admin]

    get "/dashboard", AdminController, :dashboard
  end

  scope "/auth", LogflareWeb do
    pipe_through [:browser, :require_auth]
    get "/new-api-key", AuthController, :new_api_key
  end

  scope "/auth", LogflareWeb do
    pipe_through :browser

    get "/login", AuthController, :login
    get "/logout", AuthController, :logout
    get "/:provider", AuthController, :request
    get "/:provider/callback", AuthController, :callback
  end

  # deprecate
  scope "/api", LogflareWeb do
    pipe_through :api
    post "/cloudflare/event", CloudflareController, :event
    post "/v1/cloudflare/event", CloudflareControllerV1, :event
  end

  # deprecate
  scope "/api", LogflareWeb do
    pipe_through [:api, :require_api_auth]
    post "/logs", LogController, :create
  end

  scope "/webhooks", LogflareWeb do
    pipe_through :api
    post "/cloudflare/v1", CloudflareControllerV1, :event
  end

  scope "/logs", LogflareWeb do
    pipe_through [:api, :require_api_auth]
    post "/", LogController, :create
    post "/cloudflare", LogController, :create
    post "/elixir/logger", LogController, :elixir_logger
  end
end
