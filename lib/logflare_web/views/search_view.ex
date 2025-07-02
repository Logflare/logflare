defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers
  import Logflare.Lql.Utils
  import Logflare.Utils, only: [iso_timestamp: 1]
  import LogflareWeb.SearchLive.DisplayTimezoneComponent
  alias Logflare.DateTimeUtils
  alias LogflareWeb.Search
end
