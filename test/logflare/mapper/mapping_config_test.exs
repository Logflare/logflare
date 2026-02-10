defmodule Logflare.Mapper.MappingConfigTest do
  use ExUnit.Case, async: true

  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.MappingConfig.InferCondition
  alias Logflare.Mapper.MappingConfig.InferRule
  alias Logflare.Mapper.MappingConfig.PickEntry

  describe "FieldConfig constructors" do
    test "string/2 creates correct struct" do
      field = Field.string("trace_id", paths: ["$.trace_id", "$.traceId"], default: "")

      assert %FieldConfig{} = field
      assert field.name == "trace_id"
      assert field.type == "string"
      assert field.paths == ["$.trace_id", "$.traceId"]
      assert field.default == ""
    end

    test "string/2 with transform" do
      field = Field.string("severity_text", path: "$.level", transform: "upcase")

      assert field.transform == "upcase"
      assert field.path == "$.level"
    end

    test "uint8/2" do
      field = Field.uint8("trace_flags", path: "$.trace_flags", default: 0)

      assert field.type == "uint8"
      assert field.default == "0"
    end

    test "uint32/2" do
      field = Field.uint32("flags", path: "$.flags", default: 0)

      assert field.type == "uint32"
    end

    test "uint64/2" do
      field = Field.uint64("count", path: "$.count", default: 0)

      assert field.type == "uint64"
    end

    test "int32/2" do
      field = Field.int32("scale", path: "$.scale", default: 0)

      assert field.type == "int32"
    end

    test "float64/2" do
      field = Field.float64("value", path: "$.value", default: 0.0)

      assert field.type == "float64"
    end

    test "bool/2" do
      field = Field.bool("is_monotonic", path: "$.is_monotonic", default: false)

      assert field.type == "bool"
      assert field.default == "false"
    end

    test "enum8/2 with values and infer" do
      infer_rules = [
        %InferRule{
          result: "histogram",
          any: [
            %InferCondition{path: "$.bucket_counts", predicate: "not_empty"}
          ],
          all: []
        }
      ]

      field =
        Field.enum8("metric_type",
          paths: ["$.metric_type"],
          values: %{"gauge" => 1, "sum" => 2, "histogram" => 3},
          infer: infer_rules,
          default: 1
        )

      assert field.type == "enum8"
      assert field.enum_values == %{"gauge" => 1, "sum" => 2, "histogram" => 3}
      assert length(field.infer) == 1
    end

    test "datetime64/2 with default precision" do
      field = Field.datetime64("timestamp", path: "$.timestamp")

      assert field.type == "datetime64"
      assert field.precision == 9
    end

    test "datetime64/2 with custom precision" do
      field = Field.datetime64("timestamp", path: "$.timestamp", precision: 6)

      assert field.precision == 6
    end

    test "json/2 with exclude and elevate keys" do
      field =
        Field.json("log_attributes",
          path: "$",
          exclude_keys: ["id", "timestamp"],
          elevate_keys: ["metadata"]
        )

      assert field.type == "json"
      assert field.exclude_keys == ["id", "timestamp"]
      assert field.elevate_keys == ["metadata"]
    end

    test "json/2 with pick entries" do
      field =
        Field.json("resource_attributes",
          paths: ["$.resource"],
          pick: [
            {"region", ["$.metadata.region", "$.region"]},
            {"cluster", ["$.metadata.cluster"]}
          ]
        )

      assert length(field.pick) == 2
      assert %PickEntry{key: "region", paths: ["$.metadata.region", "$.region"]} = hd(field.pick)
    end

    test "from_output option" do
      field =
        Field.uint8("severity_number",
          from_output: "severity_text",
          value_map: %{"INFO" => 9, "ERROR" => 17},
          default: 0
        )

      assert field.from_output == "severity_text"
      assert field.value_map == %{"INFO" => 9, "ERROR" => 17}
    end
  end

  describe "MappingConfig.new/1" do
    test "creates config from field list" do
      config =
        MappingConfig.new([
          Field.string("name", path: "$.name"),
          Field.uint8("age", path: "$.age", default: 0)
        ])

      assert %MappingConfig{} = config
      assert length(config.fields) == 2
    end
  end

  describe "MappingConfig.to_nif_map/1" do
    test "serializes basic config" do
      config =
        MappingConfig.new([
          Field.string("name", path: "$.name", default: ""),
          Field.uint8("age", path: "$.age", default: 0)
        ])

      nif_map = MappingConfig.to_nif_map(config)

      assert %{"fields" => fields} = nif_map
      assert length(fields) == 2

      [name_field, age_field] = fields
      assert name_field["name"] == "name"
      assert name_field["type"] == "string"
      assert name_field["path"] == "$.name"
      assert name_field["default"] == ""

      assert age_field["name"] == "age"
      assert age_field["type"] == "uint8"
      assert age_field["default"] == 0
    end

    test "serializes config with pick entries" do
      config =
        MappingConfig.new([
          Field.json("attrs",
            paths: ["$.resource"],
            pick: [{"region", ["$.region", "$.metadata.region"]}]
          )
        ])

      nif_map = MappingConfig.to_nif_map(config)
      [field] = nif_map["fields"]

      assert [%{"key" => "region", "paths" => ["$.region", "$.metadata.region"]}] = field["pick"]
    end

    test "serializes config with infer rules" do
      config =
        MappingConfig.new([
          Field.enum8("metric_type",
            paths: ["$.metric_type"],
            values: %{"gauge" => 1, "histogram" => 3},
            infer: [
              %InferRule{
                result: "histogram",
                any: [
                  %InferCondition{path: "$.bucket_counts", predicate: "not_empty"}
                ],
                all: []
              }
            ],
            default: 1
          )
        ])

      nif_map = MappingConfig.to_nif_map(config)
      [field] = nif_map["fields"]

      assert field["enum_values"] == %{"gauge" => 1, "histogram" => 3}

      assert [%{"result" => "histogram", "any" => [%{"path" => "$.bucket_counts"}]}] =
               field["infer"]
    end

    test "numeric defaults are serialized as numbers" do
      config =
        MappingConfig.new([
          Field.float64("value", path: "$.value", default: 0.0)
        ])

      nif_map = MappingConfig.to_nif_map(config)
      [field] = nif_map["fields"]

      assert field["default"] == 0.0
    end
  end
end
