defmodule Logflare.Validator.BigQuery.SchemaChangeTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Google.BigQuery.SchemaFactory
  import Logflare.Validator.BigQuery.SchemaChange

  describe "bigquery bigquery schema change validation" do
    test "valid?/1 returns true for valid params" do
    end

    test "correctly creates a typemap from schema" do
        assert :schema
               |> build(:third)
               |> to_typemap(from: :bigquery_schema) == typemap_for_third()
    end

    test "correctly builds a typemap from metadata" do
        assert :metadata
               |> build(:third)
               |> to_typemap(from: :bigquery_schema) == typemap_for_third()
    end

    test "valid? returns true for correct metadata and schema" do
      schema = build(:schema, :third)
      metadata = build(:metadata, :third)

      assert valid?(metadata, schema)
    end
  end

  def typemap_for_third() do
    %{
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
end
