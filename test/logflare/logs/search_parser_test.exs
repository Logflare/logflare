defmodule Logflare.Logs.Search.ParserTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.Search.Parser

  describe "Parser parse" do
    test "simple message search string" do
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "~", path: "metadata.message", value: "user"},
               %{operator: "~", path: "metadata.message", value: "sign"},
               %{operator: "~", path: "metadata.message", value: "up"}
             ]
    end
  end
end
