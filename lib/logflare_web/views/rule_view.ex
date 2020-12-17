defmodule LogflareWeb.RuleView do
  use LogflareWeb, :view
  alias LogflareWeb.{Source, Lql}
  alias LogflareWeb.SharedView
  alias LogflareWeb.Sources.BqSchemaLive
  import Logflare.Rules, only: [has_regex_rules?: 1]

  import LogflareWeb.Helpers.Modals
end
