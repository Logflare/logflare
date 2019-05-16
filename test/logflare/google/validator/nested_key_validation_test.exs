defmodule Logflare.Google.BigQuery.Validator.NestedKeysTest do
  use ExUnit.Case
  import Logflare.Google.BigQuery.Validator.NestedKeys

  describe "list of maps keys validation" do
    test "returns false for repeated keys with different types" do
      payload1 = %{
        "stacktrace" => [
          %{"key1" => "string"},
          %{"key1" => "string2"}
        ]
      }

      payload2 = %{
        "stacktrace" => [
          %{"key1" => "string"},
          %{"key1" => 1}
        ]
      }

      assert valid?(payload1)
      refute valid?(payload2)
    end
  end
end
