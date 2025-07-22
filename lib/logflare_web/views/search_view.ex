defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  import LogflareWeb.ModalLiveHelpers
  import Logflare.Lql.Rules
  import Logflare.Utils, only: [iso_timestamp: 1]
  import LogflareWeb.SearchLive.TimezoneComponent
end
