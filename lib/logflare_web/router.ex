defmodule LogflareWeb.Router do
  @moduledoc false
  use LogflareWeb, :router
  use PhoenixOauth2Provider.Router, otp_app: :logflare

  import Phoenix.LiveDashboard.Router
  import Phoenix.LiveView.Router

  alias LogflareWeb.BertParser
  alias LogflareWeb.JsonParser
  alias LogflareWeb.SyslogParser
  alias LogflareWeb.NdjsonParser
  alias LogflareWeb.ProtobufParser

  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest

  @common_on_mount_hooks if Application.compile_env(:logflare, :sql_sandbox),
                           do: [LogflareWeb.Hooks.AllowTestSandbox],
                           else: []

  @dashboard_hooks [LogflareWeb.AuthLive]

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
           worker-src 'self' blob:;\
           style-src 'self' 'unsafe-inline' https://use.fontawesome.com https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://api.github.com;\
           img-src 'self' data: https://*.googleusercontent.com https://www.gravatar.com https://avatars.githubusercontent.com https://platform.slack-edge.com;\
           font-src 'self' data: https://use.fontawesome.com https://cdn.jsdelivr.net;\
           frame-src 'self' https://platform.twitter.com https://install.cloudflareapps.com https://datastudio.google.com https://js.stripe.com https://www.youtube.com https://lookerstudio.google.com/;\
           """
         end).(),
      "referrer-policy" => "same-origin"
    })

    plug(LogflareWeb.Plugs.SetTeamIfNil)
    plug(LogflareWeb.Plugs.SetTeamContext)
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
      parsers: [JsonParser, BertParser, SyslogParser, NdjsonParser],
      json_decoder: Jason,
      body_reader: {PlugCaisson, :read_body, []},
      length: 12_000_000
    )

    plug(:accepts, ["json", "bert"])
    plug(LogflareWeb.Plugs.SetHeaders)
    plug(OpenApiSpex.Plug.PutApiSpec, module: LogflareWeb.ApiSpec)
  end

  pipeline :otlp_api do
    plug(Plug.RequestId)

    plug(Plug.Parsers,
      parsers: [ProtobufParser],
      json_decoder: Jason,
      body_reader: {PlugCaisson, :read_body, []},
      length: 12_000_000
    )

    plug(:accepts, ["json", "protobuf"])
    plug(LogflareWeb.Plugs.SetHeaders)
    plug(LogflareWeb.Plugs.BlockSystemSource)
  end

  pipeline :require_endpoint_auth do
    plug(LogflareWeb.Plugs.VerifyApiAccess)
    plug(LogflareWeb.Plugs.FetchResource)
    plug(LogflareWeb.Plugs.VerifyResourceAccess)
  end

  pipeline :require_ingest_api_auth do
    plug(LogflareWeb.Plugs.VerifyApiAccess)
    plug(LogflareWeb.Plugs.FetchResource)
    plug(LogflareWeb.Plugs.VerifyResourceAccess)
    # We are ensuring source start in Logs.ingest
    # plug LogflareWeb.Plugs.EnsureSourceStarted
    plug(LogflareWeb.Plugs.SetPlanFromCache)
    plug(LogflareWeb.Plugs.RateLimiter)
    plug(LogflareWeb.Plugs.BufferLimiter)
  end

  pipeline :require_mgmt_api_auth do
    plug(LogflareWeb.Plugs.VerifyApiAccess, scopes: ~w(private))
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

  pipeline :auth_switch do
    plug(LogflareWeb.Plugs.AuthSwitch)
  end

  pipeline :partner_api do
    plug(LogflareWeb.Plugs.VerifyApiAccess, scopes: ~w(partner))
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

    live_session :dashboard, on_mount: @common_on_mount_hooks ++ @dashboard_hooks do
      live("/dashboard", DashboardLive, :index)
      live("/access-tokens", AccessTokensLive, :index)
      live("/backends", BackendsLive, :index)
      live("/backends/new", BackendsLive, :new)
      live("/backends/:id", BackendsLive, :show)
      live("/backends/:id/edit", BackendsLive, :edit)
      live("/query", QueryLive, :index)
      live("/key-values", KeyValuesLive, :index)

      scope "/integrations" do
        live("/vercel/edit", VercelLogDrainsLive, :edit)
      end
    end
  end

  scope "/endpoints", LogflareWeb do
    pipe_through([:browser, :require_auth])

    live_session :endpoints, on_mount: @common_on_mount_hooks ++ @dashboard_hooks do
      live("/", EndpointsLive, :index)
      live("/new", EndpointsLive, :new)
      live("/:id", EndpointsLive, :show)
      live("/:id/edit", EndpointsLive, :edit)
    end
  end

  scope "/alerts", LogflareWeb do
    pipe_through([:browser, :require_auth])

    live_session :alerts, on_mount: @common_on_mount_hooks ++ @dashboard_hooks do
      live "/", AlertsLive, :index
      live "/new", AlertsLive, :new
      live "/:id", AlertsLive, :show
      live "/:id/edit", AlertsLive, :edit
    end
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

  scope "/teams", LogflareWeb do
    pipe_through([:browser, :require_auth])

    get("/switch", TeamController, :switch)
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
      live_session(:rules,
        on_mount: @common_on_mount_hooks ++ @dashboard_hooks,
        root_layout: {LogflareWeb.LayoutView, :root}
      ) do
        live("/rules", Sources.RulesLive)
      end
    end

    get("/:id/test-alerts", SourceController, :test_alerts)
    get("/:id/test-slack-hook", SourceController, :test_slack_hook)
    get("/:id/delete-slack-hook", SourceController, :delete_slack_hook)
    get("/:id/rejected", SourceController, :rejected_logs)
    live("/:source_id/search", Source.SearchLV)
    live("/:source_id/event", LogEventLive, :show)
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
  end

  scope "/billing", LogflareWeb do
    pipe_through([:browser, :require_auth])

    post("/", BillingController, :create)
    delete("/", BillingController, :delete)
    live("/edit", BillingAccountLive.Edit, :edit)
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
    pipe_through([:browser, :require_auth, :check_admin])

    get("/dashboard", AdminController, :dashboard)
    get("/accounts", AdminController, :accounts)
    live("/cluster", Admin.ClusterLive, :index)
    live("/partner", Admin.PartnerLive, :index)

    get("/plans", AdminPlanController, :index)
    get("/plans/new", AdminPlanController, :new)
    post("/plans", AdminPlanController, :create)
    get("/plans/:id/edit", AdminPlanController, :edit)
    put("/plans/:id", AdminPlanController, :update)

    delete("/accounts/:id", AdminController, :delete_account)
    get("/accounts/:id/become", AdminController, :become_account)

    live_dashboard("/livedashboard", ecto_repos: [], metrics: Logflare.Telemetry)
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
    get("/login/single_tenant", AuthController, :single_tenant_signin)
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
    get("/query", Api.QueryController, :query)
    get("/query/parse", Api.QueryController, :parse)

    resources("/access-tokens", Api.AccessTokenController,
      param: "token",
      only: [:index, :create, :delete]
    )

    resources("/sources", Api.SourceController,
      param: "token",
      only: [:index, :show, :create, :update, :delete]
    ) do
      get "/schema", Api.SourceController, :show_schema
      get "/recent", Api.SourceController, :recent
      post "/backends/:backend_token", Api.SourceController, :add_backend
      delete "/backends/:backend_token", Api.SourceController, :remove_backend
    end

    resources("/rules", Api.RuleController,
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

    scope "/backends" do
      resources("/", Api.BackendController,
        param: "token",
        only: [:index, :show, :create, :update, :delete]
      )

      post("/:token/test", Api.BackendController, :test_connection)
    end

    get "/key-values", Api.KeyValueController, :index
    post "/key-values", Api.KeyValueController, :create
    delete "/key-values", Api.KeyValueController, :delete
  end

  scope "/api/partner", LogflareWeb do
    pipe_through([:api, :partner_api])

    get("/users", Api.Partner.UserController, :index)
    post("/users", Api.Partner.UserController, :create)
    put("/users/:user_token/upgrade", Api.Partner.UserController, :upgrade)
    put("/users/:user_token/downgrade", Api.Partner.UserController, :downgrade)

    get("/users/:user_token", Api.Partner.UserController, :get_user)
    get("/users/:user_token/usage", Api.Partner.UserController, :get_user_usage)

    delete("/users/:user_token", Api.Partner.UserController, :delete_user)
  end

  scope "/api" do
    pipe_through(:api)

    get("/openapi", OpenApiSpex.Plug.RenderSpec, [])
  end

  scope "/swaggerui" do
    pipe_through(:browser)
    get("/", OpenApiSpex.Plug.SwaggerUI, path: "/api/openapi")
  end

  scope "/api/endpoints", LogflareWeb, assigns: %{resource_type: :endpoint} do
    pipe_through([:api, :require_endpoint_auth])
    get("/query/:token_or_name", EndpointsController, :query)
    post("/query/:token_or_name", EndpointsController, :query)

    # deprecated
    get("/query/name/:name", EndpointsController, :query)
  end

  # legacy route
  scope "/endpoints/query", LogflareWeb, assigns: %{resource_type: :endpoint} do
    pipe_through([:api, :require_endpoint_auth])
    get("/:token_or_name", EndpointsController, :query)
  end

  scope "/v1", LogflareWeb, assigns: %{resource_type: :source} do
    pipe_through([:otlp_api, :require_ingest_api_auth])

    post(
      "/traces",
      LogController,
      :otel_traces,
      assigns: %{protobuf_schema: ExportTraceServiceRequest}
    )

    post(
      "/metrics",
      LogController,
      :otel_metrics,
      assigns: %{protobuf_schema: ExportMetricsServiceRequest}
    )

    post(
      "/logs",
      LogController,
      :otel_logs,
      assigns: %{protobuf_schema: ExportLogsServiceRequest}
    )
  end

  for path <- ["/logs", "/api/logs", "/api/events"] do
    scope path, LogflareWeb, assigns: %{resource_type: :source} do
      pipe_through([:api, :require_ingest_api_auth])

      post("/", LogController, :create)
      options("/", LogController, :create)
      post("/browser/reports", LogController, :browser_reports)
      options("/browser/reports", LogController, :browser_reports)
      post("/json", LogController, :generic_json)
      options("/json", LogController, :generic_json)
      post("/cloud-event", LogController, :cloud_event)
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

    # logpush
    scope "#{path}/cloudflare", LogflareWeb, assigns: %{resource_type: :source} do
      pipe_through([:logpush, :api, :require_ingest_api_auth])
      post("/", LogController, :cloudflare)
    end
  end

  if Application.compile_env(:logflare, :dev_routes) do
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
