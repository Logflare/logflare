defmodule Logflare.Google.BigQuery.Validator.NestedValuesTest do
  use ExUnit.Case
  import Logflare.Google.BigQuery.Validator.NestedValues

  describe "nested values validation" do
    test "diverging types in stacktrace.[0,1].key1.key2" do
      payload = fn v ->
        %{
          "stacktrace" => [
            %{"key1" => %{"key2" => v}, "key11" => "string"},
            %{"key1" => %{"key2" => "string"}, "key12" => 0}
          ]
        }
      end

      assert valid?(payload.("string"))
      refute valid?(payload.(1))
    end

    test "diverging types in stacktrace.[0,1].key1.[0].key2" do
      payload = fn v ->
        %{
          "stacktrace" => [
            %{"key1" => [%{"key2" => v}]},
            %{"key1" => [%{"key2" => "string"}]}
          ]
        }
      end

      assert valid?(payload.("string"))
      refute valid?(payload.(1))
    end
  end
end
