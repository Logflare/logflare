defmodule LogtailWeb.PageController do
  use LogtailWeb, :controller

  def index(conn, _params) do
    render conn, "index.html"
  end
end
