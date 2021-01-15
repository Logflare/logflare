defmodule LogflareWeb.RuleView do
  use LogflareWeb, :view
  alias LogflareWeb.Lql
  alias LogflareWeb.LqlHelpers
  alias LogflareWeb.SharedView
  import LogflareWeb.ModalLiveHelpers
  import Logflare.Rules, only: [has_regex_rules?: 1]
end
