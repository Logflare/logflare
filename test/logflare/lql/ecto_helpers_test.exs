defmodule Logflare.Lql.EctoHelpersTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  doctest Logflare.Lql.EctoHelpers, import: true
end
