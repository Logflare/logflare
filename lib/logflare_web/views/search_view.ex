defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  import LogflareWeb.Helpers.BqSchema
  import LogflareWeb.ModalLiveHelpers
  import Logflare.Lql.Utils
  alias Logflare.DateTimeUtils
  alias LogflareWeb.Search
end
