defmodule Logflare.Validator.BigQuery.SchemaChangeTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Google.BigQuery.SchemaFactory
  import Logflare.Validator.BigQuery.SchemaChange

  describe "bigquery bigquery schema change validation" do
    test "correctly creates a typemap from schema" do
      schema = build(:table, :third)
      typemap = bq_schema_to_typemap(schema)

      assert typemap === %{
               timestamp: %{t: :timestamp},
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
                           street: %{
                             t: %{t: :string},
                             city: %{city: :string},
                             st: %{t: :string}
                           }
                         }
                       }
                     }
                   }
                 }
               }
             }
    end
  end
end
