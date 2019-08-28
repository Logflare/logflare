defmodule Logflare.Validator.BigQuerySchemaSpecTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Logs.Validators.BigQuerySchemaSpec

  @valid_payload %{
    "datacenter" => "aws",
    "ip_address" => "100.100.100.100",
    "request_headers" => %{
      "connection" => "close",
      "servers" => %{
        "blah" => "water",
        "home" => "not home",
        "deep_nest" => [
          %{"more_deep_nest" => %{"a" => 1}},
          %{"more_deep_nest2" => %{"a" => 2}}
        ]
      },
      "user_agent" => "chrome"
    },
    "request_method" => "POST"
  }

  describe "validation of payloads for building of BigQuery schema" do
    test "doesn't reject a valid payload" do
      assert valid?(@valid_payload)
    end

    test "doesn't reject payload with capitalized fields" do
      payload = %{
        "message" => "Set IAM policy: 5 accounts",
        "metadata" => %{
          "Logflare" => %{
            "Google" => %{
              "CloudResourceManager" => %{
                "set_iam_policy_0" => %{
                  "accounts" => 5,
                  "response" => "ok"
                }
              }
            }
          }
        }
      }

      assert valid?(payload)
    end

    test "rejects nested key starting from a digit" do
      refute @valid_payload
             |> Map.put("nested1", %{"1datacenter" => "aws"})
             |> valid?()
    end

    test "rejects nested key longer than 128 symbols" do
      long_key = String.duplicate("x", 129)

      refute @valid_payload
             |> Map.put("nested1", %{long_key => "aws"})
             |> valid?
    end

    test "rejects key starting from a special character" do
      refute @valid_payload
             |> Map.put("nested1", %{"l̈" => "aws"})
             |> valid?
    end

    test "rejects keys starting with a reserved keyword" do
      refute valid?(%{"_TABLE_field" => "val"})
      refute valid?(%{"_FILE_field" => "val"})
      refute valid?(%{"_PARTITION_field" => "val"})
    end

    test "rejects nested invalid key in a list" do
      nested = %{"datacenter" => "aws", "valid" => [%{"3field" => "not"}, %{"key" => "valid"}]}

      refute @valid_payload
             |> Map.put("nested1", %{"nested2" => nested})
             |> valid?
    end

    test "rejects two identical downcased keys" do
      nested = %{"datacenter" => "aws", "dataCenter" => "aws2"}

      refute @valid_payload
             |> Map.put("nested1", %{"nested2" => nested})
             |> valid?
    end
  end
end
