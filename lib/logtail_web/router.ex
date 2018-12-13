defmodule LogtailWeb.Router do
  use LogtailWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_flash
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug LogtailWeb.Plugs.SetUser
  end

  pipeline :api do
    plug :accepts, ["json"]

  end

  scope "/", LogtailWeb do
    pipe_through :browser # Use the default browser stack

    get "/", SourceController, :index
    get "/logs/:id", LogController, :show

  end

  scope "/sources", LogtailWeb do
    pipe_through :browser

    get "/", SourceController, :sources
    get "/new", SourceController, :new
    post "/", SourceController, :create

  end

  scope "/auth", LogtailWeb do
    pipe_through :browser

    get "/logout", AuthController, :logout
    get "/:provider", AuthController, :request
    get "/:provider/callback", AuthController, :callback
  end

  scope "/api", LogtailWeb do
    pipe_through :api
    post "/logs", LogController, :create
  end
end
