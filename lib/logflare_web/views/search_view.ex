defmodule LogflareWeb.SearchView do
  use LogflareWeb, :view

  import LogflareWeb.Helpers.Notifications
  import LogflareWeb.Helpers.Modals
  import LogflareWeb.Helpers.BqSchema
  import Logflare.Lql.Utils

  import PhoenixLiveReact, only: [live_react_component: 2]

  alias LogflareWeb.Source.SearchLV.ModalLVC
end
