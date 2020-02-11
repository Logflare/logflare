defmodule Logflare.Logs.Validators.EqDeepFieldTypesTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.Validators.EqDeepFieldTypes

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
      assert valid?(key_lvl3_payload("string", "string"))
      refute valid?(key_lvl3_payload(1, "string"))
      refute valid?(key_lvl3_payload("string", true))
      refute valid?(key_lvl3_payload(1, %{}))
    end

    test "diverging list types in key0.[2,3].key2.[1,2].key_lvl3" do
      assert valid?(key_lvl3_payload(["string1", "string2"], ["string3", "string4", "string5"]))
      assert valid?(key_lvl3_payload([], ["string3", "string4", "string5"]))
      assert valid?(key_lvl3_payload([[1]], [[2, 3]]))
      assert valid?(key_lvl3_payload([1], []))

      refute valid?(key_lvl3_payload([1, []], []))
      refute valid?(key_lvl3_payload([1], [1, "2"]))
      refute valid?(key_lvl3_payload([[1]], [["2"]]))
      refute valid?(key_lvl3_payload([["string1"]], ["string2"]))
      refute valid?(key_lvl3_payload([1, 2, 3], ["string"]))
      refute valid?(key_lvl3_payload("string", ["string"]))
      refute valid?(key_lvl3_payload([1], ["1"]))
    end

    def key_lvl3_payload(v1, v2) do
      %{
        "key0" => [
          %{"key1" => "string"},
          %{"key1" => "string"},
          %{"key2" => %{"keylvl3" => v1}},
          %{
            "key2" => [
              %{"keylvl3" => v1},
              %{"keylvl3.1" => "string"}
            ]
          },
          %{
            "key2" => [
              %{"keylvl3.1" => "string"},
              %{"keylvl3" => v2}
            ]
          }
        ]
      }
    end

    test "diverging list types in single map key" do
      assert valid?(single_path(["string1", "string2"], ["string3", "string4", "string5"]))
      assert valid?(single_path([], ["string3", "string4", "string5"]))
      assert valid?(single_path([[1]], [[2, 3]]))
      assert valid?(single_path([1], []))

      refute valid?(single_path(1, "1"))
      refute valid?(single_path([1, []], []))
      refute valid?(single_path([1], [1, "2"]))
      refute valid?(single_path("string", ["string"]))
    end
  end

  def single_path(v1, v2) do
    %{"key0" => "string1", "key1" => %{"key2" => [v1, v2]}}
  end
end
