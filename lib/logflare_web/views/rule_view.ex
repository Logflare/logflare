defmodule LogflareWeb.RuleView do
  use LogflareWeb, :view

  import Logflare.Rules, only: [has_regex_rules?: 1]

  import LogflareWeb.Helpers.Flash
end
