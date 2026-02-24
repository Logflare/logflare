defmodule Logflare.Validator.BigQuerySchemaChangeTest do
  @moduledoc false
  use Logflare.DataCase

  import Logflare.Logs.Validators.BigQuerySchemaChange

  import Logflare.Google.BigQuery.SchemaUtils

  alias Logflare.LogEvent, as: LE
  # alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaFactory
  alias Logflare.Factory
  alias Logflare.Sources

  @moduletag :failing

  describe "bigquery schema change validation" do
    test "validate/1 returns :ok with no metadata in BQ schema" do
      u1 = Factory.insert(:user)
      s1 = Factory.insert(:source, user_id: u1.id)
      s1 = Sources.get_by(id: s1.id)
      # schema = SchemaBuilder.initial_table_schema()
      # allow Sources.Cache.get_bq_schema(s1), return: schema

      le =
        LE.make(
          %{
            "message" => "valid_test_message",
            "metadata" => %{
              "user" => %{
                "name" => "name one"
              }
            }
          },
          %{source: s1}
        )

      assert validate(le, s1) === :ok
    end

    test "correctly creates a typemap from schema" do
      assert :schema
             |> SchemaFactory.build(variant: :third)
             |> to_typemap(from: :bigquery_schema) == typemap_for_third()
    end

    test "correctly builds a typemap from metadata" do
      assert :metadata
             |> SchemaFactory.build(variant: :third)
             |> to_typemap() == typemap_for_third().metadata.fields
    end

    test "try_merge returns :ok for correct metadata and schema" do
      schema = SchemaFactory.build(:schema, variant: :third) |> bq_schema_to_flat_typemap()

      m = SchemaFactory.build(:metadata, variant: :third)

      metadata =
        to_typemap(%{metadata: m})
        |> flatten_typemap()

      assert try_merge(metadata, schema) == :ok
    end

    test "valid? returns false for various changed nested field types" do
      schema = SchemaFactory.build(:schema, variant: :third)

      event = SchemaFactory.build(:metadata, variant: :third)

      metadata =
        event
        |> put_in(~w[user address city], 1000)

      metadata2 =
        event
        |> put_in(~w[user vip], :not_boolean_atom)

      metadata3 =
        event
        |> put_in(~w[ip_address], %{"field" => 1})

      refute valid?(metadata, schema)
      refute valid?(metadata2, schema)
      refute valid?(metadata3, schema)
    end

    test "valid? returns false for various changed nested list field types" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)

      event = SchemaFactory.build(:metadata, variant: :third_with_lists)

      metadata =
        event
        |> put_in(~w[user address cities], ["Amsterdam"])

      assert valid?(metadata, schema)

      metadata =
        event
        |> put_in(~w[user address cities], "Amsterdam")

      refute valid?(metadata, schema)

      metadata =
        event
        |> put_in(~w[user address cities], [1])

      refute valid?(metadata, schema)

      metadata =
        event
        |> put_in(~w[user ids], [10_000])

      assert valid?(metadata, schema)

      metadata =
        event
        |> put_in(~w[user ids], ["10000"])

      refute valid?(metadata, schema)

      metadata =
        event
        |> put_in(~w[user ids], [10_000.0])

      refute valid?(metadata, schema)
    end

    test "correctly handles empty containers in metadata" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      metadata = SchemaFactory.build(:metadata, variant: :third_with_lists)
      metadata = put_in(metadata, ["user", "ids"], [])
      metadata = put_in(metadata, ["user", "address"], %{})

      assert valid?(metadata, schema)

      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      metadata = SchemaFactory.build(:metadata, variant: :third_with_lists)
      metadata = put_in(metadata, ["user", "ids"], [[]])
      metadata = put_in(metadata, ["user", "address"], [%{}])

      assert valid?(metadata, schema)
    end

    test "correctly handles invalid utf8 / valid latin1 fields" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      metadata = SchemaFactory.build(:metadata, variant: :third_with_lists)

      metadata =
        put_in(
          metadata,
          ["user", "address"],
          %{
            <<95, 67, 195, 95, 100, 105, 103, 111, 95, 100, 101, 95, 82, 97, 115, 116, 114, 101,
              105, 111>> => "valid",
            "field2" =>
              <<95, 67, 195, 95, 100, 105, 103, 111, 95, 100, 101, 95, 82, 97, 115, 116, 114, 101,
                105, 111>>
          }
        )

      assert valid?(metadata, schema)
    end
  end

  def typemap_for_third do
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
