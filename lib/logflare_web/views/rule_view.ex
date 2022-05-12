defmodule LogflareWeb.RuleView do
  use LogflareWeb, :view
  alias LogflareWeb.Lql
  alias LogflareWeb.LqlHelpers
  import LogflareWeb.ModalLiveHelpers
  import Logflare.Rules, only: [has_regex_rules?: 1]
end
