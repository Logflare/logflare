defmodule Logflare.Logs.Search.ParserTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Logs.Search.Parser

  describe "Parser parse" do
    test "simple message search string" do
      str = ~S|user sign up|
      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "~", path: "message", value: "user"},
               %{operator: "~", path: "message", value: "sign"},
               %{operator: "~", path: "message", value: "up"}
             ]
    end

    test "quoted message search string" do
      str = ~S|new "user sign up" server|
      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "~", path: "message", value: "user sign up"},
               %{operator: "~", path: "message", value: "new"},
               %{operator: "~", path: "message", value: "server"}
             ]
    end

    test "nested fields filter" do
      str = ~S|
        metadata.user.type:paid
        metadata.user.id:<1
        metadata.user.views:<=1
        metadata.users.source_count:>100
        metadata.context.error_count:>=100
        metadata.user.about:~referrall
      |

      {:ok, result} = Parser.parse(str)

      assert result == [
               %{operator: "=", path: "metadata.user.type", value: "paid"},
               %{operator: "<", path: "metadata.user.id", value: "1"},
               %{operator: "<=", path: "metadata.user.views", value: "1"},
               %{operator: ">", path: "metadata.users.source_count", value: "100"},
               %{operator: ">=", path: "metadata.context.error_count", value: "100"},
               %{operator: "~", path: "metadata.user.about", value: "referrall"}
             ]
    end
  end
end
