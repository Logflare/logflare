defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  import LogflareWeb.ModalLiveHelpers
  import Logflare.Lql.Rules
  import LogflareWeb.SearchLive.TimezoneComponent
end
