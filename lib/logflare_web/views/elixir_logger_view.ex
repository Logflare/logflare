defmodule LogflareWeb.ElixirLoggerView do
  use LogflareWeb, :view

  # alias LogflareWeb.ElixirLoggerView

  def render("index.json", %{message: message}) do
    %{message: message}
  end
end
