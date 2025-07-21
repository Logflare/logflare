defmodule LqlParserTestHelpers do
  @moduledoc """
  Test helper for LQL parser modules that need to import parser combinators.

  This centralizes the common imports needed when testing NimbleParsec-based
  parser modules that depend on functions from multiple parser modules.
  """

  defmacro __using__(_) do
    quote do
      import NimbleParsec
      import Logflare.Lql.Parser.BasicCombinators
      import Logflare.Lql.Parser.ChartParsers
      import Logflare.Lql.Parser.ClauseBuilders
      import Logflare.Lql.Parser.DateTimeParsers
      import Logflare.Lql.Parser.RuleBuilders
      import Logflare.Lql.Parser.Validators
    end
  end
end
