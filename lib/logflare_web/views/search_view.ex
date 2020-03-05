defmodule LogflareWeb.SearchView do
  use LogflareWeb, :view

  import LogflareWeb.Helpers.Flash
  import LogflareWeb.Helpers.Modals
  import LogflareWeb.Helpers.BqSchema

  import PhoenixLiveReact, only: [live_react_component: 2]
end
