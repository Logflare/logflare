defmodule Logflare.UtilsTest do
  use ExUnit.Case, async: true

  doctest Logflare.EnumDeepUpdate, import: true
  doctest Logflare.Utils, import: true
  doctest Logflare.Utils.Map, import: true
end
