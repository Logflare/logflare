defmodule Logflare.Google.BigQuery.Validator.NestedValuesTest do
  use ExUnit.Case
  import Logflare.Google.BigQuery.Validator.NestedValues

  describe "nested values validation" do
    test "diverging types in key0.[0,1].key1.key2" do
      payload = fn v ->
        %{
          "key0" => [
            %{"key1" => %{"key2" => v}, "key11" => "string"},
            %{"key1" => %{"key2" => "string"}, "key12" => 0}
          ]
        }
      end

      assert valid?(payload.("string"))
      refute valid?(payload.(1))
    end

    test "diverging types in key0.[0,1].key1.[0].key2" do
      payload = fn v ->
        %{
          "key0" => [
            %{"key1" => [%{"key2" => v}]},
            %{"key1" => [%{"key2" => "string"}]}
          ]
        }
      end

      assert valid?(payload.("string"))
      refute valid?(payload.(1))
    end

    test "diverging types in key0.[2,3].key2.[1,2].key_lvl3" do
      payload = fn v1, v2 ->
        %{
          "key0" => [
            %{"key1" => "string"},
            %{"key1" => "string"},
            %{
              "key2" => [
                %{"keylvl3" => v1},
                %{"keylvl3" => "string"}
              ]
            },
            %{
              "key2" => [
                %{"keylvl3" => "string"},
                %{"keylvl3" => v2}
              ]
            }
          ]
        }
      end

      assert valid?(payload.("string", "string"))
      refute valid?(payload.(1, "string"))
      refute valid?(payload.("string", true))
      refute valid?(payload.(1, %{}))
    end
  end
end
