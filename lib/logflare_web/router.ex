defmodule LogflareWeb.Router do
  @moduledoc false
  use LogflareWeb, :router
  alias LogflareWeb.LayoutView
  use PhoenixOauth2Provider.Router, otp_app: :logflare
  import Phoenix.LiveView.Router

  # TODO: move plug calls in SourceController and RuleController into here

  pipeline :browser do
    plug LogflareWeb.Plugs.SetNodeHeader
    plug Plug.RequestId
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_flash
    plug :fetch_live_flash
    plug :put_root_layout, {LogflareWeb.LayoutView, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug LogflareWeb.Plugs.SetVerifyUser
    plug LogflareWeb.Plugs.SetTeamIfNil
    plug LogflareWeb.Plugs.SetTeamUser
    plug LogflareWeb.Plugs.SetTeam
    plug LogflareWeb.Plugs.SetPlan
  end

  pipeline :api do
    plug LogflareWeb.Plugs.MaybeContentTypeToJson

    plug Plug.Parsers,
      parsers: [:json, :bert],
      json_decoder: Jason

    plug :accepts, ["json", "bert"]
  end

  pipeline :require_ingest_api_auth do
    plug LogflareWeb.Plugs.SetVerifyUser
    plug LogflareWeb.Plugs.SetVerifySource
    plug LogflareWeb.Plugs.SetPlanFromCache
    plug LogflareWeb.Plugs.RateLimiter
  end

  pipeline :require_mgmt_api_auth do
    plug LogflareWeb.Plugs.SetVerifyUser
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

  pipeline :check_owner do
    plug LogflareWeb.Plugs.AuthMustBeOwner
  end

  pipeline :check_team_user do
    plug LogflareWeb.Plugs.CheckTeamUser
  end

  pipeline :auth_switch do
    plug LogflareWeb.Plugs.AuthSwitch
  end

  # Oauth2 Provider Routes
  scope "/" do
    pipe_through [:api, :oauth_public]
    oauth_api_routes()
  end

  # Oauth2 Provider Routes
  scope "/" do
    pipe_through [:browser, :require_auth]
    oauth_routes()
  end

  # Oauth2 Provider Routes for Vercel and Cloudflare
  scope "/oauth/token", LogflareWeb do
    post "/vercel", Auth.OauthProviderController, :vercel_grant
    post "/zeit", Auth.OauthProviderController, :vercel_grant
    post "/cloudflare", Auth.OauthProviderController, :cloudflare_grant
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
    get "/slack-app-setup", MarketingController, :slack_app_setup
    get "/vercel-setup", MarketingController, :vercel_setup
  end

  scope "/", LogflareWeb do
    pipe_through [:browser, :require_auth]
    get "/dashboard", SourceController, :dashboard
  end

  scope "/sources", LogflareWeb do
    pipe_through :browser
    get "/public/:public_token", SourceController, :public
    get "/:id/unsubscribe/:token", Auth.UnsubscribeController, :unsubscribe
    get "/:id/unsubscribe/stranger/:token", Auth.UnsubscribeController, :unsubscribe_stranger
    get "/:id/unsubscribe/team-member/:token", Auth.UnsubscribeController, :unsubscribe_team_user
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :require_auth]

    resources "/", SourceController, except: [:index] do
      live "/rules", Sources.RulesLV, layout: {LogflareWeb.LayoutView, :root}
      delete "/saved-searches/:id", SavedSearchesController, :delete
    end

    delete "/:id/force-delete", SourceController, :del_source_and_redirect
    get "/:id/test-alerts", SourceController, :test_alerts
    get "/:id/test-slack-hook", SourceController, :test_slack_hook
    get "/:id/delete-slack-hook", SourceController, :delete_slack_hook
    get "/:id/rejected", SourceController, :rejected_logs
    live "/:source_id/search", Source.SearchLV
    get "/:id/favorite", SourceController, :favorite
    get "/:id/clear", SourceController, :clear_logs
    get "/:id/explore", SourceController, :explore
  end

  scope "/profile", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_team_user]

    get "/edit", TeamUserController, :edit
    put "/edit", TeamUserController, :update
    delete "/", TeamUserController, :delete_self
  end

  scope "/profile/:id", LogflareWeb do
    pipe_through [:browser, :require_auth]
    delete "/", TeamUserController, :delete
  end

  scope "/profile/switch", LogflareWeb do
    pipe_through [:browser, :require_auth, :auth_switch]
    get "/", TeamUserController, :change_team
  end

  scope "/account", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    get "/edit", UserController, :edit
    put "/edit", UserController, :update
    delete "/", UserController, :delete
    get "/new-api-key", UserController, :new_api_key
  end

  scope "/account/billing", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    post "/", BillingController, :create
    delete "/", BillingController, :delete
    get "/edit", BillingController, :edit
    post "/sync", BillingController, :sync
  end

  scope "/account/billing/subscription", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    get "/subscribed", BillingController, :success
    get "/abandoned", BillingController, :abandoned
    delete "/", BillingController, :unsubscribe
    get "/confirm", BillingController, :confirm_subscription
    get "/confirm/change", BillingController, :change_subscription
    get "/updated-payment-method", BillingController, :update_credit_card_success
  end

  scope "/admin", LogflareWeb do
    pipe_through [:browser, :check_admin]

    get "/dashboard", AdminController, :dashboard
    get "/sources", AdminController, :sources
    live "/search", AdminSearchDashboardLive, layout: {LayoutView, :root}
    get "/cluster", AdminClusterController, :index
    get "/plans", AdminPlanController, :index
    get "/plans/new", AdminPlanController, :new
    post "/plans/new", AdminPlanController, :create
    get "/plans/:id/edit", AdminPlanController, :edit
    put "/plans/:id/edit", AdminPlanController, :update
  end

  scope "/install", LogflareWeb do
    pipe_through :browser

    get "/vercel", Auth.VercelAuth, :set_oauth_params
    get "/zeit", Auth.VercelAuth, :set_oauth_params
  end

  scope "/auth", LogflareWeb do
    pipe_through :browser

    get "/login", AuthController, :login
    get "/login/email", Auth.EmailController, :login
    post "/login/email", Auth.EmailController, :send_link
    get "/login/email/verify", Auth.EmailController, :verify_token
    post "/login/email/verify", Auth.EmailController, :callback
    get "/logout", AuthController, :logout
    get "/:provider", Auth.OauthController, :request
    get "/email/callback/:token", Auth.EmailController, :callback
    get "/:provider/callback", Auth.OauthController, :callback
  end

  scope "/webhooks", LogflareWeb do
    pipe_through :api
    post "/cloudflare/v1", CloudflareControllerV1, :event
    post "/stripe", StripeController, :event
  end

  scope "/health", LogflareWeb do
    pipe_through :api
    get "/", HealthCheckController, :check
  end

  # Account management API.
  scope "/api", LogflareWeb do
    pipe_through [:api, :require_mgmt_api_auth]
    get "/account", UserController, :api_show
    get "/sources", SourceController, :api_index
  end

  # Old log ingest endpoint. Deprecate.
  scope "/api/logs", LogflareWeb do
    pipe_through [:api, :require_ingest_api_auth]
    post "/", LogController, :create
  end

  # Log ingest goes through https://api.logflare.app/logs
  scope "/logs", LogflareWeb do
    pipe_through [:api, :require_ingest_api_auth]
    post "/", LogController, :create
    options "/", LogController, :create
    post "/browser/reports", LogController, :browser_reports
    options "/browser/reports", LogController, :browser_reports
    post "/json", LogController, :generic_json
    options "/json", LogController, :generic_json
    post "/cloudflare", LogController, :cloudflare
    post "/zeit", LogController, :vercel_ingest
    post "/vercel", LogController, :vercel_ingest
    post "/elixir/logger", LogController, :elixir_logger
  end
end
