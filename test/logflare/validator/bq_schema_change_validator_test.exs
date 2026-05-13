defmodule Logflare.Validator.BigQuerySchemaChangeTest do
  @moduledoc false
  use Logflare.DataCase

  import Logflare.Logs.Validators.BigQuerySchemaChange

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.Factory
  alias Logflare.Google.BigQuery.SchemaFactory
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.LogEvent, as: LE
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  describe "validate/2" do
    setup do
      Factory.insert(:plan)
      :ok
    end

    test "returns :ok when the source has no cached schema" do
      user = Factory.insert(:user)
      source = Factory.insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))

      le =
        LE.make(
          %{
            "message" => "valid_test_message",
            "metadata" => %{"user" => %{"name" => "name one"}}
          },
          %{source: source}
        )

      assert validate(le, source) === :ok
    end

    test "returns :ok for a body matching the cached schema (integer timestamp is skipped)" do
      user = Factory.insert(:user)
      source = Factory.insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))

      bq_schema = SchemaFactory.build(:schema, variant: :third)
      schema_flat_map = SchemaUtils.bq_schema_to_flat_typemap(bq_schema)

      SourceSchemas.create_or_update_source_schema(source, %{
        bigquery_schema: bq_schema,
        schema_flat_map: schema_flat_map
      })

      le =
        LE.make(
          %{"metadata" => SchemaFactory.build(:metadata, variant: :third)},
          %{source: source}
        )

      assert validate(le, source) === :ok
    end

    test "correctly builds a typemap from metadata" do
      assert :metadata
             |> SchemaFactory.build(variant: :third)
             |> SchemaUtils.to_typemap() == typemap_for_third()["metadata"].fields
    end

    test "returns {:error, _} when a leaf type conflicts with the cached schema" do
      user = Factory.insert(:user)
      source = Factory.insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))

      bq_schema = SchemaFactory.build(:schema, variant: :third)
      schema_flat_map = SchemaUtils.bq_schema_to_flat_typemap(bq_schema)

      SourceSchemas.create_or_update_source_schema(source, %{
        bigquery_schema: bq_schema,
        schema_flat_map: schema_flat_map
      })

      conflicting_metadata =
        :metadata
        |> SchemaFactory.build(variant: :third)
        |> put_in(~w[user address city], 1000)

      le = LE.make(%{"metadata" => conflicting_metadata}, %{source: source})

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "metadata.user.address.city"
    end

    test "short-circuits when source.validate_schema is false (no cache lookup)" do
      # No DB-backed source; the first function clause should match before
      # any cache access. validate_schema: false is the contract.
      source = %Source{id: 0, validate_schema: false}
      le = %LE{body: %{"any" => 123, "field" => "value"}, valid: true}
      assert validate(le, source) == :ok
    end

    test "validates nested 'timestamp' fields (top-level skip does not leak)" do
      source =
        source_with_flat_map(%{
          "metadata" => :map,
          "metadata.event" => :map,
          "metadata.event.timestamp" => :string
        })

      le =
        LE.make(
          %{"metadata" => %{"event" => %{"timestamp" => 12_345}}},
          %{source: source}
        )

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "metadata.event.timestamp"
    end

    test "skips top-level 'start_time' and 'end_time' integers against :datetime schema" do
      source =
        source_with_flat_map(%{
          "start_time" => :datetime,
          "end_time" => :datetime
        })

      le =
        LE.make(
          %{"start_time" => 1_700_000_000_000_000_000, "end_time" => 1_700_000_000_000_000_001},
          %{source: source}
        )

      assert validate(le, source) == :ok
    end

    test "validates nested 'start_time' / 'end_time' fields (top-level skip does not leak)" do
      source =
        source_with_flat_map(%{
          "metadata" => :map,
          "metadata.span" => :map,
          "metadata.span.start_time" => :string
        })

      le =
        LE.make(
          %{"metadata" => %{"span" => %{"start_time" => 12_345}}},
          %{source: source}
        )

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "metadata.span.start_time"
    end

    test "heterogeneous list-of-maps with non-map element raises Type error, not BadMapError" do
      source = source_with_flat_map(%{"items" => :map, "items.a" => :integer})

      le = LE.make(%{"items" => [%{"a" => 1}, "stray"]}, %{source: source})

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "items"
      refute message =~ "expected a map"
    end

    test "accepts integer value against :float schema (BQ INT64 -> FLOAT64 coercion)" do
      source = source_with_flat_map(%{"metadata" => :map, "metadata.value" => :float})

      le = LE.make(%{"metadata" => %{"value" => 42}}, %{source: source})

      assert validate(le, source) == :ok
    end

    test "accepts list of integers against {:list, :float} schema" do
      source =
        source_with_flat_map(%{"metadata" => :map, "metadata.bounds" => {:list, :float}})

      le = LE.make(%{"metadata" => %{"bounds" => [1, 2, 3]}}, %{source: source})

      assert validate(le, source) == :ok
    end

    test "accepts mixed int/float list against {:list, :float} schema" do
      source =
        source_with_flat_map(%{"metadata" => :map, "metadata.bounds" => {:list, :float}})

      le = LE.make(%{"metadata" => %{"bounds" => [1, 2.5, 3]}}, %{source: source})

      assert validate(le, source) == :ok
    end

    test "rejects mixed int/float list against {:list, :integer} schema" do
      source =
        source_with_flat_map(%{"metadata" => :map, "metadata.bounds" => {:list, :integer}})

      le = LE.make(%{"metadata" => %{"bounds" => [1, 2.5, 3]}}, %{source: source})

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "metadata.bounds"
    end

    test "still rejects :float incoming against :integer schema (no reverse coercion)" do
      source = source_with_flat_map(%{"metadata" => :map, "metadata.count" => :integer})

      le = LE.make(%{"metadata" => %{"count" => 1.5}}, %{source: source})

      assert {:error, message} = validate(le, source)
      assert message =~ "Type error"
      assert message =~ "metadata.count"
    end
  end

  describe "valid?/2" do
    test "returns true for correct metadata against its own schema" do
      schema = SchemaFactory.build(:schema, variant: :third)
      m = SchemaFactory.build(:metadata, variant: :third)
      assert valid?(%{"metadata" => m}, schema)
    end

    test "returns false for various changed nested field types" do
      schema = SchemaFactory.build(:schema, variant: :third)
      event = SchemaFactory.build(:metadata, variant: :third)

      city_to_integer = put_in(event, ~w[user address city], 1000)
      vip_to_atom = put_in(event, ~w[user vip], :not_boolean_atom)
      ip_to_map = put_in(event, ~w[ip_address], %{"field" => 1})

      refute valid?(%{"metadata" => city_to_integer}, schema)
      refute valid?(%{"metadata" => vip_to_atom}, schema)
      refute valid?(%{"metadata" => ip_to_map}, schema)
    end

    test "returns false for various changed nested list field types" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      event = SchemaFactory.build(:metadata, variant: :third_with_lists)

      assert valid?(
               %{"metadata" => put_in(event, ~w[user address cities], ["Amsterdam"])},
               schema
             )

      refute valid?(%{"metadata" => put_in(event, ~w[user address cities], "Amsterdam")}, schema)
      refute valid?(%{"metadata" => put_in(event, ~w[user address cities], [1])}, schema)
      assert valid?(%{"metadata" => put_in(event, ~w[user ids], [10_000])}, schema)
      refute valid?(%{"metadata" => put_in(event, ~w[user ids], ["10000"])}, schema)
      refute valid?(%{"metadata" => put_in(event, ~w[user ids], [10_000.0])}, schema)
    end

    test "skips empty containers in metadata" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      base = SchemaFactory.build(:metadata, variant: :third_with_lists)

      m1 =
        base
        |> put_in(["user", "ids"], [])
        |> put_in(["user", "address"], %{})

      m2 =
        base
        |> put_in(["user", "ids"], [[]])
        |> put_in(["user", "address"], [%{}])

      assert valid?(%{"metadata" => m1}, schema)
      assert valid?(%{"metadata" => m2}, schema)
    end

    test "treats invalid utf8 / valid latin1 keys as new fields (no type conflict)" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      base = SchemaFactory.build(:metadata, variant: :third_with_lists)

      latin1 =
        <<95, 67, 195, 95, 100, 105, 103, 111, 95, 100, 101, 95, 82, 97, 115, 116, 114, 101, 105,
          111>>

      metadata =
        put_in(base, ["user", "address"], %{
          latin1 => "valid",
          "field2" => latin1
        })

      assert valid?(%{"metadata" => metadata}, schema)
    end

    test "validates each entry in a REPEATED RECORD list (list of maps)" do
      bq_schema =
        SchemaBuilder.build_table_schema(
          %{"events" => [%{"level" => "info"}]},
          SchemaBuilder.initial_table_schema()
        )

      assert valid?(
               %{"events" => [%{"level" => "warn"}, %{"level" => "error"}]},
               bq_schema
             )

      refute valid?(
               %{"events" => [%{"level" => "warn"}, %{"level" => 5}]},
               bq_schema
             )
    end

    test "validates each entry in a REPEATED scalar list" do
      schema = SchemaFactory.build(:schema, variant: :third_with_lists)
      base = SchemaFactory.build(:metadata, variant: :third_with_lists)

      assert valid?(%{"metadata" => base}, schema)

      mixed_ids = put_in(base, ["user", "ids"], [299, "not_an_int", 12])
      refute valid?(%{"metadata" => mixed_ids}, schema)

      mixed_dates =
        put_in(base, ["user", "last_login_datetimes"], ["2020-01-01T00:00:01Z", 12_345])

      refute valid?(%{"metadata" => mixed_dates}, schema)
    end

    test "DateTime and NaiveDateTime values satisfy a :datetime schema type" do
      bq_schema = %TS{
        fields: [
          %TFS{
            name: "metadata",
            type: "RECORD",
            mode: "NULLABLE",
            fields: [
              %TFS{name: "created_at", type: "TIMESTAMP", mode: "NULLABLE"}
            ]
          }
        ]
      }

      assert valid?(%{"metadata" => %{"created_at" => DateTime.utc_now()}}, bq_schema)
      assert valid?(%{"metadata" => %{"created_at" => NaiveDateTime.utc_now()}}, bq_schema)
      refute valid?(%{"metadata" => %{"created_at" => 12_345}}, bq_schema)
    end
  end

  defp source_with_flat_map(schema_flat_map) do
    user = Factory.insert(:user)
    source = Factory.insert(:source, user_id: user.id) |> then(&Sources.get_by(id: &1.id))

    {:ok, _} =
      SourceSchemas.create_or_update_source_schema(source, %{
        bigquery_schema: SchemaBuilder.initial_table_schema(),
        schema_flat_map: schema_flat_map
      })

    source
  end

  defp typemap_for_third do
    %{
      "timestamp" => %{t: :datetime},
      "event_message" => %{t: :string},
      "metadata" => %{
        t: :map,
        fields: %{
          "datacenter" => %{t: :string},
          "ip_address" => %{t: :string},
          "request_method" => %{t: :string},
          "user" => %{
            t: :map,
            fields: %{
              "browser" => %{t: :string},
              "id" => %{t: :integer},
              "vip" => %{t: :boolean},
              "company" => %{t: :string},
              "login_count" => %{t: :integer},
              "address" => %{
                t: :map,
                fields: %{
                  "street" => %{t: :string},
                  "city" => %{t: :string},
                  "st" => %{t: :string}
                }
              }
            }
          }
        }
      }
    }
  end
end
