defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  import LogflareWeb.Helpers.Modals
  import LogflareWeb.Helpers.BqSchema
  import Logflare.Lql.Utils
  alias Logflare.DateTimeUtils

  alias LogflareWeb.Source.SearchLV.ModalLVC
end
