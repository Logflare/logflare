defmodule LogflareWeb.BillingAccountView do
  import LogflareWeb.Helpers.Forms
  import Logflare.Sources, only: [count_for_billing: 1]
  use LogflareWeb, :live_view_with_templates
end
