defmodule LogtailWeb.Router do
  use LogtailWeb, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_flash
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]

  end

  scope "/", LogtailWeb do
    pipe_through :browser # Use the default browser stack
    get "/", PageController, :index

  end

  scope "/api", LogtailWeb do
    pipe_through :api
    post "/logs", LogController, :create
  end
end
