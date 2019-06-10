defmodule Logflare.Validator.BigQuerySchemaChangeTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.Google.BigQuery.SchemaFactory
  import Logflare.Logs.Validators.BigQuerySchemaChange

  describe "bigquery bigquery schema change validation" do
    test "valid?/1 returns true for valid params" do
    end

    test "correctly creates a typemap from schema" do
      assert :schema
             |> build(variant: :third)
             |> to_typemap(from: :bigquery_schema) == typemap_for_third()
    end

    test "correctly builds a typemap from metadata" do
      assert :metadata
             |> build(variant: :third)
             |> to_typemap() == typemap_for_third()
    end

    test "valid? returns true for correct metadata and schema" do
      schema = build(:schema, variant: :third)
      metadata = build(:metadata, variant: :third)

      assert valid?(metadata, schema)
    end

    test "valid? returns false for various changed nested field types" do
      schema = build(:schema, variant: :third)

      event = build(:metadata, variant: :third)

      metadata =
        event
        |> put_in(~w[metadata user address city], 1000)

      metadata2 =
        event
        |> put_in(~w[metadata user vip], :not_boolean_atom)

      metadata3 =
        event
        |> put_in(~w[metadata ip_address], %{"field" => 1})

      refute valid?(metadata, schema)
      refute valid?(metadata2, schema)
      refute valid?(metadata3, schema)
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
