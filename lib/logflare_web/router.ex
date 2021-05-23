defmodule LogflareWeb.Router do
  @moduledoc false
  use LogflareWeb, :router
  alias LogflareWeb.LayoutView
  use PhoenixOauth2Provider.Router, otp_app: :logflare
  import Phoenix.LiveView.Router

  # TODO: move plug calls in SourceController and RuleController into here

  pipeline :browser do
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
    plug LogflareWeb.Plugs.EnsureSourceStarted
    plug LogflareWeb.Plugs.SetHeaders
  end

  pipeline :api do
    plug Plug.RequestId
    plug LogflareWeb.Plugs.MaybeContentTypeToJson

    plug Plug.Parsers,
      parsers: [:json, :bert, :syslog],
      json_decoder: Jason

    plug :accepts, ["json", "bert"]
    plug LogflareWeb.Plugs.SetHeaders
  end

  pipeline :require_ingest_api_auth do
    plug LogflareWeb.Plugs.SetVerifyUser
    plug LogflareWeb.Plugs.SetVerifySource
    # We are ensuring source start in Logs.ingest
    # plug LogflareWeb.Plugs.EnsureSourceStarted
    plug LogflareWeb.Plugs.SetPlanFromCache
    plug LogflareWeb.Plugs.RateLimiter
  end

  pipeline :require_mgmt_api_auth do
    plug LogflareWeb.Plugs.SetVerifyUser
  end

  pipeline :require_auth do
    plug LogflareWeb.Plugs.RequireAuth
  end

  pipeline :set_source do
    plug LogflareWeb.Plugs.SetVerifySource
  end

  pipeline :ensure_source_started do
    plug LogflareWeb.Plugs.EnsureSourceStarted
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
    get "/pricing", MarketingController, :pricing
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
    pipe_through [:browser]

    get "/:id/unsubscribe/:token", Auth.UnsubscribeController, :unsubscribe
    get "/:id/unsubscribe/stranger/:token", Auth.UnsubscribeController, :unsubscribe_stranger
    get "/:id/unsubscribe/team-member/:token", Auth.UnsubscribeController, :unsubscribe_team_user
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :set_source, :ensure_source_started]

    get "/public/:public_token", SourceController, :public
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :require_auth]

    get "/new", SourceController, :new
    post "/", SourceController, :create
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :require_auth, :set_source]

    delete "/:id", SourceController, :delete
    delete "/:id/force-delete", SourceController, :del_source_and_redirect
  end

  scope "/sources", LogflareWeb do
    pipe_through [:browser, :require_auth, :set_source, :ensure_source_started]

    resources "/", SourceController, except: [:index, :new, :create, :delete] do
      live "/rules", Sources.RulesLV, layout: {LogflareWeb.LayoutView, :root}
      delete "/saved-searches/:id", SavedSearchesController, :delete
    end

    get "/:id/test-alerts", SourceController, :test_alerts
    get "/:id/test-slack-hook", SourceController, :test_slack_hook
    get "/:id/delete-slack-hook", SourceController, :delete_slack_hook
    get "/:id/rejected", SourceController, :rejected_logs
    live "/:source_id/search", Source.SearchLV
    live "/:source_id/event", LogEventLive.Show, :show
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
    pipe_through [:browser, :require_auth]

    post "/", AuthController, :create_and_sign_in
  end

  scope "/account", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    get "/edit", UserController, :edit
    put "/edit", UserController, :update
    delete "/", UserController, :delete
    get "/edit/api-key", UserController, :new_api_key
    put "/edit/owner", UserController, :change_owner
  end

  scope "/account/billing", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    post "/", BillingController, :create
    delete "/", BillingController, :delete
    live "/edit", BillingAccountLive, :edit
    get "/sync", BillingController, :sync
  end

  scope "/account/billing/subscription", LogflareWeb do
    pipe_through [:browser, :require_auth, :check_owner]

    get "/subscribed", BillingController, :success
    get "/abandoned", BillingController, :abandoned
    delete "/", BillingController, :unsubscribe
    get "/confirm", BillingController, :confirm_subscription
    get "/confirm/change", BillingController, :update_payment_details
    get "/updated-payment-method", BillingController, :update_credit_card_success
    get "/manage", BillingController, :portal
    get "/change", BillingController, :change_subscription
  end

  scope "/admin", LogflareWeb do
    pipe_through [:browser, :check_admin]

    get "/dashboard", AdminController, :dashboard
    get "/sources", AdminController, :sources
    get "/accounts", AdminController, :accounts
    live "/search", AdminSearchDashboardLive, layout: {LayoutView, :root}
    get "/cluster", AdminClusterController, :index

    get "/plans", AdminPlanController, :index
    get "/plans/new", AdminPlanController, :new
    post "/plans/new", AdminPlanController, :create
    get "/plans/:id/edit", AdminPlanController, :edit
    put "/plans/:id/edit", AdminPlanController, :update

    delete "/accounts/:id", AdminController, :delete_account
    get "/accounts/:id/become", AdminController, :become_account
  end

  scope "/install", LogflareWeb do
    pipe_through :browser

    get "/vercel", Auth.VercelAuth, :set_oauth_params
    get "/vercel-v2", Auth.VercelAuth, :set_oauth_params_v2
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
    # post "/vercel", VercelController, :event
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
    post "/typecasts", LogController, :create_with_typecasts
    post "/logplex", LogController, :syslog
    post "/syslogs", LogController, :syslog

    # Deprecate after September 1, 2020
    post "/syslog", LogController, :syslog
  end
end
