defmodule LogflareWeb.Router do
  @moduledoc false
  use LogflareWeb, :router
  alias LogflareWeb.LayoutView
  use PhoenixOauth2Provider.Router, otp_app: :logflare
  import Phoenix.LiveView.Router

  # TODO: move plug calls in SourceController and RuleController into here

  pipeline :browser do
    plug(Plug.RequestId)
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_flash)
    plug(:fetch_live_flash)
    plug(:put_root_layout, {LogflareWeb.LayoutView, :root})
    plug(:protect_from_forgery)

    plug(:put_secure_browser_headers, %{
      "content-security-policy" =>
        (fn ->
           """
           \
           default-src 'self';\
           connect-src 'self' #{if Application.compile_env(:logflare, :env) == :prod, do: "wss://logflare.app", else: "ws://localhost:4000"} https://api.github.com;\
           script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://buttons.github.io https://platform.twitter.com https://cdnjs.cloudflare.com https://js.stripe.com;\
           style-src 'self' 'unsafe-inline' https://use.fontawesome.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://api.github.com;\
           img-src 'self' data: https://*.googleusercontent.com https://www.gravatar.com https://avatars.githubusercontent.com https://platform.slack-edge.com;\
           font-src 'self' https://use.fontawesome.com;\
           frame-src 'self' https://platform.twitter.com https://install.cloudflareapps.com https://datastudio.google.com https://js.stripe.com https://www.youtube.com https://lookerstudio.google.com/;\
           """
         end).(),
      "referrer-policy" => "same-origin"
    })

    plug(LogflareWeb.Plugs.SetVerifyUser)
    plug(LogflareWeb.Plugs.SetTeamIfNil)
    plug(LogflareWeb.Plugs.SetTeamUser)
    plug(LogflareWeb.Plugs.SetTeam)
    plug(LogflareWeb.Plugs.SetPlan)
    plug(LogflareWeb.Plugs.EnsureSourceStarted)
    plug(LogflareWeb.Plugs.SetHeaders)
  end

  pipeline :logpush do
    plug(:handle_logpush_headers)
  end

  pipeline :api do
    plug(Plug.RequestId)
    plug(LogflareWeb.Plugs.MaybeContentTypeToJson)

    plug(Plug.Parsers,
      parsers: [:json, :bert, :syslog, :ndjson],
      json_decoder: Jason
    )

    plug(:accepts, ["json", "bert"])
    plug(LogflareWeb.Plugs.SetHeaders)
    plug(OpenApiSpex.Plug.PutApiSpec, module: LogflareWeb.ApiSpec)
  end

  pipeline :require_ingest_api_auth do
    plug(LogflareWeb.Plugs.SetVerifyUser)
    plug(LogflareWeb.Plugs.SetVerifySource)
    # We are ensuring source start in Logs.ingest
    # plug LogflareWeb.Plugs.EnsureSourceStarted
    plug(LogflareWeb.Plugs.SetPlanFromCache)
    plug(LogflareWeb.Plugs.RateLimiter)
  end

  pipeline :require_mgmt_api_auth do
    plug(LogflareWeb.Plugs.SetVerifyUser)
  end

  pipeline :require_auth do
    plug(LogflareWeb.Plugs.RequireAuth)
  end

  pipeline :set_source do
    plug(LogflareWeb.Plugs.SetVerifySource)
  end

  pipeline :ensure_source_started do
    plug(LogflareWeb.Plugs.EnsureSourceStarted)
  end

  pipeline :oauth_public do
    plug(:accepts, ["json"])
    plug(:put_secure_browser_headers, %{"content-security-policy" => "default-src 'self'"})
  end

  pipeline :check_admin do
    plug(LogflareWeb.Plugs.CheckAdmin)
  end

  pipeline :check_owner do
    plug(LogflareWeb.Plugs.AuthMustBeOwner)
  end

  pipeline :check_team_user do
    plug(LogflareWeb.Plugs.CheckTeamUser)
  end

  pipeline :api_auth_endpoints do
    plug(LogflareWeb.Plugs.VerifyApiAccess, resource: :endpoints)
  end

  pipeline :auth_switch do
    plug(LogflareWeb.Plugs.AuthSwitch)
  end

  # Oauth2 Provider Routes
  scope "/" do
    pipe_through([:api, :oauth_public])
    oauth_api_routes()
  end

  # Oauth2 Provider Routes
  scope "/" do
    pipe_through([:browser, :require_auth])
    oauth_routes()
  end

  # Oauth2 Provider Routes for Vercel and Cloudflare
  scope "/oauth/token", LogflareWeb do
    post("/vercel", Auth.OauthProviderController, :vercel_grant)
    post("/zeit", Auth.OauthProviderController, :vercel_grant)
    post("/cloudflare", Auth.OauthProviderController, :cloudflare_grant)
  end

  scope "/", LogflareWeb do
    pipe_through(:browser)
    get("/", MarketingController, :index)
    get("/pricing", MarketingController, :pricing)
    get("/terms", MarketingController, :terms)
    get("/privacy", MarketingController, :privacy)
    get("/cookies", MarketingController, :cookies)
    get("/contact", MarketingController, :contact)
  end

  scope "/guides", LogflareWeb do
    pipe_through(:browser)
    get("/", MarketingController, :guides)
    get("/overview", MarketingController, :overview)
    get("/bigquery-setup", MarketingController, :big_query_setup)
    get("/data-studio-setup", MarketingController, :data_studio_setup)
    get("/event-analytics", MarketingController, :event_analytics_demo)
    get("/slack-app-setup", MarketingController, :slack_app_setup)
    get("/vercel-setup", MarketingController, :vercel_setup)
  end

  scope "/", LogflareWeb do
    pipe_through([:browser, :require_auth])
    get("/dashboard", SourceController, :dashboard)
  end

  scope "/endpoints/query", LogflareWeb do
    pipe_through([:api, :api_auth_endpoints])
    get("/:token", EndpointsController, :query)
  end

  scope "/endpoints", LogflareWeb do
    pipe_through([:browser, :require_auth])

    get("/", EndpointsController, :index)
    post("/", EndpointsController, :create)

    get("/new", EndpointsController, :new)
    get("/apply", EndpointsController, :apply)
    get("/:id", EndpointsController, :show)
    get("/:id/edit", EndpointsController, :edit)
    put("/:id", EndpointsController, :update)
    put("/:id/reset_url", EndpointsController, :reset_url)
    delete("/:id", EndpointsController, :delete)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser])

    get("/:id/unsubscribe/:token", Auth.UnsubscribeController, :unsubscribe)
    get("/:id/unsubscribe/stranger/:token", Auth.UnsubscribeController, :unsubscribe_stranger)
    get("/:id/unsubscribe/team-member/:token", Auth.UnsubscribeController, :unsubscribe_team_user)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser, :set_source, :ensure_source_started])

    get("/public/:public_token", SourceController, :public)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser, :require_auth])

    get("/new", SourceController, :new)
    post("/", SourceController, :create)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser, :require_auth, :set_source])

    delete("/:id", SourceController, :delete)
    delete("/:id/force-delete", SourceController, :del_source_and_redirect)
  end

  scope "/sources", LogflareWeb do
    pipe_through([:browser, :require_auth, :set_source, :ensure_source_started])

    resources "/", SourceController, except: [:index, :new, :create, :delete] do
      live("/rules", Sources.RulesLV, layout: {LogflareWeb.LayoutView, :root})
      delete("/saved-searches/:id", SavedSearchesController, :delete)
    end

    get("/:id/test-alerts", SourceController, :test_alerts)
    get("/:id/test-slack-hook", SourceController, :test_slack_hook)
    get("/:id/delete-slack-hook", SourceController, :delete_slack_hook)
    get("/:id/rejected", SourceController, :rejected_logs)
    live("/:source_id/search", Source.SearchLV)
    live("/:source_id/event", LogEventLive.Show, :show)
    get("/:id/favorite", SourceController, :favorite)
    get("/:id/clear", SourceController, :clear_logs)
    get("/:id/explore", SourceController, :explore)
    post("/:id/toggle-schema-lock", SourceController, :toggle_schema_lock)
    post("/:id/toggle-schema-validation", SourceController, :toggle_schema_validation)
  end

  scope "/profile", LogflareWeb do
    pipe_through([:browser, :require_auth, :check_team_user])

    get("/edit", TeamUserController, :edit)
    put("/edit", TeamUserController, :update)
    delete("/", TeamUserController, :delete_self)
  end

  scope "/profile/:id", LogflareWeb do
    pipe_through([:browser, :require_auth])

    delete("/", TeamUserController, :delete)
  end

  scope "/profile/switch", LogflareWeb do
    pipe_through([:browser, :require_auth, :auth_switch])

    get("/", TeamUserController, :change_team)
  end

  scope "/account", LogflareWeb do
    pipe_through([:browser, :require_auth])

    post("/", AuthController, :create_and_sign_in)
  end

  scope "/account", LogflareWeb do
    pipe_through([:browser, :require_auth, :check_owner])

    get("/edit", UserController, :edit)
    put("/edit", UserController, :update)
    delete("/", UserController, :delete)
    get("/edit/api-key", UserController, :new_api_key)
    put("/edit/owner", UserController, :change_owner)

    # access token management
    live("/access-tokens", AccessTokensLive, :index)
  end

  scope "/integrations", LogflareWeb do
    pipe_through([:browser, :require_auth])

    live("/vercel/edit", VercelLogDrainsLive, :edit)
  end

  scope "/billing", LogflareWeb do
    pipe_through([:browser, :require_auth])

    post("/", BillingController, :create)
    delete("/", BillingController, :delete)
    live("/edit", BillingAccountLive, :edit)
    get("/sync", BillingController, :sync)
  end

  scope "/billing/subscription", LogflareWeb do
    pipe_through([:browser, :require_auth])

    get("/subscribed", BillingController, :success)
    get("/abandoned", BillingController, :abandoned)
    delete("/", BillingController, :unsubscribe)
    get("/confirm", BillingController, :confirm_subscription)
    get("/confirm/change", BillingController, :update_payment_details)
    get("/updated-payment-method", BillingController, :update_credit_card_success)
    get("/manage", BillingController, :portal)
    get("/change", BillingController, :change_subscription)
  end

  scope "/admin", LogflareWeb do
    pipe_through([:browser, :check_admin])

    get("/dashboard", AdminController, :dashboard)
    get("/sources", AdminController, :sources)
    get("/accounts", AdminController, :accounts)
    live("/search", AdminSearchDashboardLive, layout: {LayoutView, :root})
    live("/cluster", Admin.ClusterLive, :index)

    get("/plans", AdminPlanController, :index)
    get("/plans/new", AdminPlanController, :new)
    post("/plans/new", AdminPlanController, :create)
    get("/plans/:id/edit", AdminPlanController, :edit)
    put("/plans/:id/edit", AdminPlanController, :update)

    delete("/accounts/:id", AdminController, :delete_account)
    get("/accounts/:id/become", AdminController, :become_account)
  end

  scope "/admin", LogflareWeb do
    pipe_through([:api])
    put("/shutdown", AdminController, :shutdown_node)
  end

  scope "/install", LogflareWeb do
    pipe_through(:browser)

    get("/vercel", Auth.VercelAuth, :set_oauth_params)
    get("/vercel-v2", Auth.VercelAuth, :set_oauth_params_v2)
    get("/zeit", Auth.VercelAuth, :set_oauth_params)
  end

  scope "/auth", LogflareWeb do
    pipe_through(:browser)

    get("/login", AuthController, :login)
    get("/login/email", Auth.EmailController, :login)
    post("/login/email", Auth.EmailController, :send_link)
    get("/login/email/verify", Auth.EmailController, :verify_token)
    get("/logout", AuthController, :logout)
    get("/:provider", Auth.OauthController, :request)
    post("/login/email/verify", Auth.EmailController, :verify_token_form)
    get("/email/callback/:token", Auth.EmailController, :callback)
    get("/:provider/callback", Auth.OauthController, :callback)
  end

  scope "/webhooks", LogflareWeb do
    pipe_through(:api)
    post("/cloudflare/v1", CloudflareControllerV1, :event)
    post("/stripe", StripeController, :event)
    # post "/vercel", VercelController, :event
  end

  scope "/health", LogflareWeb do
    pipe_through(:api)
    get("/", HealthCheckController, :check)
  end

  # Account management API.
  scope "/api", LogflareWeb do
    pipe_through([:api, :require_mgmt_api_auth])

    get("/account", UserController, :api_show)

    resources("/sources", Api.SourceController,
      param: "token",
      only: [:index, :show, :create, :update, :delete]
    )

    resources("/endpoints", Api.EndpointController,
      param: "token",
      only: [:index, :show, :create, :update, :delete]
    )

    resources("/teams", Api.TeamController,
      param: "token",
      only: [:index, :show, :create, :update, :delete]
    )
  end

  scope "/api", LogflareWeb do
    pipe_through [:api, LogflareWeb.Plugs.EnsureSuperUserAuthentication]

    resources "/accounts", Api.AccountController, param: "token", only: [:create]
  end

  scope "/api" do
    pipe_through(:api)

    get("/openapi", OpenApiSpex.Plug.RenderSpec, [])
  end

  scope "/swaggerui" do
    pipe_through(:browser)
    get("/", OpenApiSpex.Plug.SwaggerUI, path: "/api/openapi")
  end

  # Old log ingest endpoint. Deprecate.
  scope "/api/logs", LogflareWeb do
    pipe_through([:api, :require_ingest_api_auth])
    post("/", LogController, :create)
  end

  scope "/api/endpoints", LogflareWeb do
    pipe_through([:api, :api_auth_endpoints])
    get("/query/:token", EndpointsController, :query)
  end

  # Log ingest goes through https://api.logflare.app/logs
  scope "/logs", LogflareWeb do
    pipe_through([:api, :require_ingest_api_auth])
    post("/", LogController, :create)
    options("/", LogController, :create)
    post("/browser/reports", LogController, :browser_reports)
    options("/browser/reports", LogController, :browser_reports)
    post("/json", LogController, :generic_json)
    options("/json", LogController, :generic_json)
    post("/zeit", LogController, :vercel_ingest)
    post("/vercel", LogController, :vercel_ingest)
    post("/netlify", LogController, :netlify)
    post("/elixir/logger", LogController, :elixir_logger)
    post("/erlang", LogController, :elixir_logger)
    post("/erlang/logger", LogController, :elixir_logger)
    post("/erlang/lager", LogController, :elixir_logger)
    post("/typecasts", LogController, :create_with_typecasts)
    post("/logplex", LogController, :syslog)
    post("/syslogs", LogController, :syslog)
    post("/github", LogController, :github)
    post("/vector", LogController, :vector)

    # Deprecate after September 1, 2020
    post("/syslog", LogController, :syslog)
  end

  scope "/logs/cloudflare", LogflareWeb do
    pipe_through([:logpush, :api, :require_ingest_api_auth])
    post("/", LogController, :cloudflare)
  end

  if Mix.env() == :dev do
    scope "/dev" do
      pipe_through([:browser])

      forward("/mailbox", Plug.Swoosh.MailboxPreview, base_path: "/dev/mailbox")
    end
  end

  def handle_logpush_headers(conn, _opts) do
    case get_req_header(conn, "content-type") do
      ["text/plain; charset=utf-8"] ->
        put_req_header(conn, "content-type", "application/x-ndjson")

      _type ->
        conn
    end
  end
end
