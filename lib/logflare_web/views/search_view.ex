defmodule LogflareWeb.SearchView do
  use LogflareWeb, :live_view_with_templates

  alias LogflareWeb.BqSchemaHelpers
  alias LogflareWeb.ModalLiveHelpers
  alias Logflare.Lql.Utils
  alias Logflare.DateTimeUtils
  alias LogflareWeb.Search
end
