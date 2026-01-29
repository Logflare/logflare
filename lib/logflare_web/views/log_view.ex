defmodule LogflareWeb.LogView do
  use LogflareWeb, :view

  import LogflareWeb.CoreComponents, only: [log_event_permalink: 1]
  import LogflareWeb.QueryComponents, only: [quick_filter: 1]

  alias LogflareWeb.JSONViewerComponent

  def render("index.json", %{message: message}) do
    %{message: message}
  end
end
