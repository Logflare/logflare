defmodule Logflare.Mapper.MappingConfigTest do
  use ExUnit.Case, async: true

  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.MappingConfig.InferCondition
  alias Logflare.Mapper.MappingConfig.InferRule
  alias Logflare.Mapper.MappingConfig.OutputFormat
  alias Logflare.Mapper.MappingConfig.PickEntry

  describe "FieldConfig constructors" do
    test "string/2 creates correct struct" do
      field = Field.string("trace_id", paths: ["$.trace_id", "$.traceId"], default: "")

      assert %MappingConfig.FieldConfig{} = field
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

    test "pick_mode option accepts the atom or string form" do
      for mode <- [:merge, "merge"] do
        assert Field.flat_map("attrs", pick_mode: mode).pick_mode == "merge"
        assert Field.json("attrs", pick_mode: mode).pick_mode == "merge"
      end
    end

    test "pick_mode defaults to nil, and :replace is stored as nil" do
      assert Field.flat_map("attrs", paths: ["$.resource"]).pick_mode == nil
      assert Field.json("attrs", paths: ["$.resource"]).pick_mode == nil
      assert Field.flat_map("attrs", pick_mode: :replace).pick_mode == nil
      assert Field.json("attrs", pick_mode: "replace").pick_mode == nil
    end

    test "flat_map/2 with exclude and elevate keys" do
      field =
        Field.flat_map("log_attributes",
          path: "$",
          exclude_keys: ["id", "timestamp"],
          elevate_keys: ["metadata"]
        )

      assert field.type == "flat_map"
      assert field.value_type == "string"
      assert field.exclude_keys == ["id", "timestamp"]
      assert field.elevate_keys == ["metadata"]
    end

    test "flat_map/2 defaults value_type to string" do
      field = Field.flat_map("attrs", path: "$")

      assert field.value_type == "string"
    end

    test "flat_map/2 with explicit value_type" do
      field = Field.flat_map("attrs", path: "$", value_type: "string")

      assert field.value_type == "string"
    end

    test "flat_map/2 with pick entries" do
      field =
        Field.flat_map("resource_attributes",
          paths: ["$.resource"],
          pick: [
            {"region", ["$.metadata.region", "$.region"]},
            {"cluster", ["$.metadata.cluster"]}
          ]
        )

      assert field.type == "flat_map"
      assert field.value_type == "string"
      assert length(field.pick) == 2
      assert %PickEntry{key: "region"} = hd(field.pick)
    end

    test "array_flat_map/2" do
      field = Field.array_flat_map("event_attrs", path: "$.events[*].attributes")

      assert field.type == "array_flat_map"
      assert field.value_type == "string"
      assert field.path == "$.events[*].attributes"
    end

    test "array_flat_map/2 with filter_nil" do
      field = Field.array_flat_map("attrs", path: "$.items", filter_nil: true)

      assert field.type == "array_flat_map"
      assert field.value_type == "string"
      assert field.filter_nil == true
    end
  end

  describe "MappingConfig.new/2" do
    test "creates a map-output config from a field list by default" do
      config =
        MappingConfig.new([
          Field.string("name", path: "$.name"),
          Field.uint8("age", path: "$.age", default: 0)
        ])

      assert %MappingConfig{} = config
      assert length(config.fields) == 2
      assert config.output == nil
    end

    test "stores a typed output format" do
      output = OutputFormat.clickhouse_row_binary(:log)
      config = MappingConfig.new([Field.string("project")], output: output)

      assert config.output == output
    end
  end

  describe "to_json/1 and from_json/1" do
    test "round-trip preserves a basic config" do
      config =
        MappingConfig.new([
          Field.string("trace_id", paths: ["$.trace_id", "$.traceId"], default: ""),
          Field.string("severity_text", path: "$.level", transform: "upcase"),
          Field.uint8("severity_number",
            from_output: "severity_text",
            value_map: %{"INFO" => 9, "ERROR" => 17},
            default: 0
          ),
          Field.datetime64("timestamp", path: "$.timestamp"),
          Field.json("attributes",
            path: "$",
            exclude_keys: ["id", "timestamp"],
            elevate_keys: ["metadata"]
          )
        ])

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)

      assert length(restored.fields) == length(config.fields)

      Enum.zip(config.fields, restored.fields)
      |> Enum.each(fn {orig, rest} ->
        assert orig.name == rest.name
        assert orig.type == rest.type
        assert orig.path == rest.path
        assert orig.paths == rest.paths
        assert orig.default == rest.default
        assert orig.transform == rest.transform
        assert orig.from_output == rest.from_output
        assert orig.value_map == rest.value_map
        assert orig.exclude_keys == rest.exclude_keys
        assert orig.elevate_keys == rest.elevate_keys
        assert orig.precision == rest.precision
        assert orig.value_type == rest.value_type
      end)
    end

    test "round-trip preserves the output format" do
      config =
        MappingConfig.new([Field.string("project")],
          output: OutputFormat.clickhouse_row_binary(:log)
        )

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)
      assert restored.output == OutputFormat.clickhouse_row_binary(:log)
    end

    test "round-trip preserves pick entries" do
      config =
        MappingConfig.new([
          Field.json("resource_attributes",
            paths: ["$.resource"],
            pick: [
              {"region", ["$.metadata.region", "$.region"]},
              {"cluster", ["$.metadata.cluster"]}
            ]
          )
        ])

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)

      [field] = restored.fields
      assert length(field.pick) == 2

      [region, cluster] = field.pick
      assert %PickEntry{key: "region", paths: ["$.metadata.region", "$.region"]} = region
      assert %PickEntry{key: "cluster", paths: ["$.metadata.cluster"]} = cluster
    end

    test "round-trip preserves pick_mode" do
      config =
        MappingConfig.new([
          Field.flat_map("attrs",
            paths: ["$.resource"],
            pick: [{"project", ["$.project"]}],
            pick_mode: :merge,
            default: %{}
          )
        ])

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)

      [field] = restored.fields
      assert field.pick_mode == "merge"

      document = %{"project" => "p", "resource" => %{"a" => 1}}
      expected = %{"attrs" => %{"project" => "p", "a" => "1"}}

      assert Mapper.map(document, Mapper.compile!(config)) == expected
      assert Mapper.map(document, Mapper.compile!(restored)) == expected
    end

    test "round-trip preserves infer rules" do
      config =
        MappingConfig.new([
          Field.enum8("metric_type",
            paths: ["$.metric_type"],
            values: %{"gauge" => 1, "sum" => 2, "histogram" => 3},
            infer: [
              %InferRule{
                result: "histogram",
                any: [
                  %InferCondition{path: "$.bucket_counts", predicate: "not_empty"}
                ],
                all: [
                  %InferCondition{
                    path: "$.kind",
                    predicate: "equals",
                    comparison_value: "cumulative"
                  }
                ]
              }
            ],
            default: 1
          )
        ])

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)

      [field] = restored.fields
      assert field.enum_values == %{"gauge" => 1, "sum" => 2, "histogram" => 3}
      assert [rule] = field.infer
      assert rule.result == "histogram"
      assert [any_cond] = rule.any
      assert any_cond.path == "$.bucket_counts"
      assert any_cond.predicate == "not_empty"
      assert [all_cond] = rule.all
      assert all_cond.path == "$.kind"
      assert all_cond.predicate == "equals"
      assert all_cond.comparison_value == "cumulative"
    end

    test "round-trip preserves flat_map value_type" do
      config =
        MappingConfig.new([
          Field.flat_map("attrs",
            path: "$",
            exclude_keys: ["id"],
            value_type: "string"
          ),
          Field.array_flat_map("event_attrs",
            path: "$.events[*].attributes",
            value_type: "string"
          )
        ])

      assert {:ok, json} = MappingConfig.to_json(config)
      assert {:ok, restored} = MappingConfig.from_json(json)

      [flat_field, array_field] = restored.fields
      assert flat_field.type == "flat_map"
      assert flat_field.value_type == "string"
      assert array_field.type == "array_flat_map"
      assert array_field.value_type == "string"
    end

    test "from_json/1 with invalid JSON returns decode error" do
      assert {:error, %Jason.DecodeError{}} = MappingConfig.from_json("not valid json")
    end

    test "from_json/1 with invalid field type returns changeset error" do
      json =
        Jason.encode!(%{
          "fields" => [
            %{"name" => "bad_field", "type" => "invalid_type"}
          ]
        })

      assert {:error, %Ecto.Changeset{}} = MappingConfig.from_json(json)
    end

    test "from_json/1 with missing required field returns changeset error" do
      json = Jason.encode!(%{"fields" => [%{"type" => "string"}]})

      assert {:error, %Ecto.Changeset{}} = MappingConfig.from_json(json)
    end

    test "from_json/1 validates the output format" do
      json =
        Jason.encode!(%{
          "fields" => [%{"name" => "project", "type" => "string"}],
          "output" => %{"format" => "clickhouse_row_binary"}
        })

      assert {:error, %Ecto.Changeset{}} = MappingConfig.from_json(json)
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

    test "serializes the output format" do
      config =
        MappingConfig.new([Field.string("project")],
          output: OutputFormat.clickhouse_row_binary(:trace)
        )

      assert MappingConfig.to_nif_map(config)["output"] == %{
               "format" => "clickhouse_row_binary",
               "row_type" => "trace"
             }
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

    test "serializes flat_map value_type" do
      config =
        MappingConfig.new([
          Field.flat_map("attrs", path: "$", value_type: "string")
        ])

      nif_map = MappingConfig.to_nif_map(config)
      [field] = nif_map["fields"]

      assert field["type"] == "flat_map"
      assert field["value_type"] == "string"
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

  describe "string filter keys" do
    test "constructor canonicalizes atom filter keys" do
      field = Field.string("s", path: "$.s", filters: %{len_gt: 3})

      assert field.filters == %{"len_gt" => 3}
    end

    test "constructor accepts string filter keys" do
      field = Field.string("s", path: "$.s", filters: %{"len_gt" => 3})

      assert field.filters == %{"len_gt" => 3}
    end

    test "constructor rejects an unknown filter key" do
      assert_raise ArgumentError, ~r/len_gtt/, fn ->
        Field.string("s", path: "$.s", filters: %{len_gtt: 3})
      end
    end

    test "constructor rejects a non-integer length filter" do
      assert_raise ArgumentError, ~r/len_gt/, fn ->
        Field.string("s", path: "$.s", filters: %{len_gt: "3"})
      end
    end

    test "constructor rejects an unsupported char_class" do
      assert_raise ArgumentError, ~r/char_class/, fn ->
        Field.string("s", path: "$.s", filters: %{char_class: "hex"})
      end
    end

    test "filters reach the nif map after a to_json/from_json round trip" do
      config =
        MappingConfig.new([
          Field.string("s", path: "$.s", filters: %{len_gt: 3}, default: "D")
        ])

      {:ok, json} = MappingConfig.to_json(config)
      {:ok, restored} = MappingConfig.from_json(json)

      assert hd(restored.fields).filters == %{"len_gt" => 3}

      nif_field = restored |> MappingConfig.to_nif_map() |> Map.fetch!("fields") |> hd()

      assert nif_field["filters"] == %{"len_gt" => 3}
    end

    test "from_json/1 rejects an unknown filter key" do
      json = ~s({"fields":[{"name":"s","type":"string","path":"$.s","filters":{"len_gtt":3}}]})

      assert {:error, %Ecto.Changeset{valid?: false}} = MappingConfig.from_json(json)
    end
  end
end
