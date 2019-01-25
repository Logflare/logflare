defmodule LogflareWeb.Router do
  use LogflareWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_flash
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug LogflareWeb.Plugs.SetUser
  end

  pipeline :api do
    plug :accepts, ["json"]
    plug LogflareWeb.Plugs.VerifyApiRequest

  end

  scope "/", LogflareWeb do
    pipe_through :browser # Use the default browser stack

    get "/dashboard", SourceController, :dashboard
    get "/", SourceController, :index

  end

  scope "/sources", LogflareWeb do
    pipe_through :browser

    # get "/new", SourceController, :new
    # post "/", SourceController, :create
    # get "/:id", SourceController, :show
    # delete "/:id", SourceController, :delete
    # edit "/:id/edit", SourceController, :edit
    # put "/:id", SourceController, :update
    resources "/", SourceController

  end

  scope "/auth", LogflareWeb do
    pipe_through :browser

    get "/logout", AuthController, :logout
    get "/new-api-key", AuthController, :new_api_key
    get "/:provider", AuthController, :request
    get "/:provider/callback", AuthController, :callback
  end

  scope "/api", LogflareWeb do
    pipe_through :api
    post "/logs", LogController, :create
  end
end
