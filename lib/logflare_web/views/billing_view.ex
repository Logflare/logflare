defmodule LogflareWeb.BillingView do
  import LogflareWeb.Helpers.Forms
  import Logflare.Sources, only: [count_for_billing: 1]
  use LogflareWeb, :view
end
