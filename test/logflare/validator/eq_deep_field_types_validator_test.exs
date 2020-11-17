defmodule Logflare.Logs.Validators.EqDeepFieldTypesTest do
  @moduledoc false
  use ExUnit.Case
  alias Logflare.LogEvent, as: LE
  import Logflare.Logs.Validators.EqDeepFieldTypes
  @moduletag :this

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
      assert catch_throw(valid?(payload.(1))) == :type_error
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
      assert catch_throw(valid?(payload.(1))) == :type_error
    end

    test "diverging types in key0.[2,3].key2.[1,2].key_lvl3" do
      assert valid?(key_lvl3_payload("string", "string"))
      assert catch_throw(valid?(key_lvl3_payload(1, "string"))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload("string", true))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload(1, %{}))) == :type_error
    end

    test "diverging list types in key0.[2,3].key2.[1,2].key_lvl3" do
      assert valid?(key_lvl3_payload(["string1", "string2"], ["string3", "string4", "string5"]))
      assert valid?(key_lvl3_payload([], ["string3", "string4", "string5"]))
      assert valid?(key_lvl3_payload([[1]], [[2, 3]]))
      assert valid?(key_lvl3_payload([1], []))

      assert catch_throw(valid?(key_lvl3_payload([1, []], []))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload([1], [1, "2"]))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload([[1]], [["2"]]))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload([["string1"]], ["string2"]))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload([1, 2, 3], ["string"]))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload("string", ["string"]))) == :type_error
      assert catch_throw(valid?(key_lvl3_payload([1], ["1"]))) == :type_error
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

      assert catch_throw(valid?(single_path(1, "1"))) == {:type_error, [:integer, :binary]}
      assert catch_throw(valid?(single_path([1, []], []))) == {:type_error, [:integer, []]}
      assert catch_throw(valid?(single_path([1], [1, "2"]))) == {:type_error, [:integer, :binary]}

      assert catch_throw(valid?(single_path("string", ["string"]))) ==
               {:type_error, [:binary, [:binary]]}
    end

    test "with tuples" do
      assert validate(%LE{
               body: %{
                 metadata: %{
                   headers: [
                     {"accept", "*/*"},
                     {"accept-encoding", "gzip, deflate, br"},
                     {"connection", "keep-alive"},
                     {"host", "localhost:4320"},
                     {"postman-token", "128bdbd8-3780-4a89-8b51-2a3781db6e91"},
                     {"user-agent", "PostmanRuntime/7.26.5"}
                   ]
                 }
               }
             }) ==
               {:error,
                "Encountered a tuple: '{\"user-agent\", :binary}'. Payloads with Elixir tuples are not supported by Logflare API."}

      assert validate(%LE{
               body: %{
                 metadata: %{
                   header: {0, 1, 2}
                 }
               }
             }) ==
               {:error,
                "Encountered a tuple: '{0, 1, 2}'. Payloads with Elixir tuples are not supported by Logflare API."}
    end
  end

  def single_path(v1, v2) do
    %{"key0" => "string1", "key1" => %{"key2" => [v1, v2]}}
  end
end
