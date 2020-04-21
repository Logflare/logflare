defmodule LogflareWeb.SearchView do
  use LogflareWeb, :view

  import LogflareWeb.Helpers.Notifications
  import LogflareWeb.Helpers.Modals
  import LogflareWeb.Helpers.BqSchema
  import Logflare.Lql.Utils

  alias LogflareWeb.Source.SearchLV.ModalLVC
end
