defmodule Logflare.Validator.BigQuery.SchemaChangeTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Google.BigQuery.SchemaFactory
  import Logflare.Validator.BigQuery.SchemaChange

  describe "bigquery bigquery schema change validation" do
    test "correctly creates a typemap from schema" do
      schema = build(:table, :third)
      typemap = to_typemap(schema)

      assert typemap === %{
               timestamp: %{t: :datetime},
               event_message: %{t: :string},
               metadata: %{
                 t: :map,
                 fields: %{
                   datacenter: %{t: :string},
                   ip_address: %{t: :string},
                   request_method: %{t: :string},
                   user: %{
                     t: :map,
                     fields: %{
                       browser: %{t: :string},
                       id: %{t: :integer},
                       vip: %{t: :boolean},
                       company: %{t: :string},
                       login_count: %{t: :integer},
                       address: %{
                         t: :map,
                         fields: %{
                           street: %{t: :string},
                             city: %{t: :string},
                             st: %{t: :string}

                         }
                       }
                     }
                   }
                 }
               }
             }
    end

    test "correctly builds a typemap from metadata" do
      schema = build(:table, :third)

      metadata = %{
        "event_message" => "This is an example.",
        "metadata" => [
          %{
            "ip_address" => "100.100.100.100",
            "datacenter" => "aws",
            "request_method" => "POST",
            "user" => %{
              "address" => %{
                "city" => "New York",
                "st" => "NY",
                "street" => "123 W Main St"
              },
              "browser" => "Firefox",
              "company" => "Apple",
              "id" => 38,
              "login_count" => 154,
              "vip" => true
            }
          }
        ],
        "timestamp" => ~N[2019-04-12 16:44:38]
      }

      typemap = to_typemap(metadata)

      assert typemap == build(:table, :third) |> to_typemap()
    end
  end
end
