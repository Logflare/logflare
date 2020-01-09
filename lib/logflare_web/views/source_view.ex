defmodule LogflareWeb.SourceView do
  use LogflareWeb, :view
  import LogflareWeb.Helpers.Flash
  import Phoenix.LiveView
  import PhoenixLiveReact, only: [live_react_component: 2]
end
