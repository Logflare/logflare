defmodule LogflareWeb.RuleView do
  use LogflareWeb, :view
  alias LogflareWeb.{Source, Lql}

  import Logflare.Rules, only: [has_regex_rules?: 1]

  import LogflareWeb.Helpers.Notifications
  import LogflareWeb.Helpers.Modals
end
