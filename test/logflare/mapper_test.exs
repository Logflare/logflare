defmodule Logflare.MapperTest do
  use ExUnit.Case, async: true

  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field
  alias Logflare.Mapper.MappingConfig.InferCondition
  alias Logflare.Mapper.MappingConfig.InferRule

  defp compile_and_map(fields, document) do
    config = MappingConfig.new(fields)
    compiled = Mapper.compile!(config)
    Mapper.map(document, compiled)
  end

  # ── Path resolution ───────────────────────────────────────────────────

  describe "path resolution" do
    test "single path from flat map" do
      result =
        compile_and_map(
          [Field.string("name", path: "$.name")],
          %{"name" => "Alice"}
        )

      assert result["name"] == "Alice"
    end

    test "single path from nested map" do
      result =
        compile_and_map(
          [Field.string("zip", path: "$.address.zip")],
          %{"address" => %{"zip" => "12345"}}
        )

      assert result["zip"] == "12345"
    end

    test "coalesce: first non-null wins" do
      result =
        compile_and_map(
          [Field.string("id", paths: ["$.trace_id", "$.traceId"])],
          %{"traceId" => "abc123"}
        )

      assert result["id"] == "abc123"
    end

    test "coalesce: first path matches" do
      result =
        compile_and_map(
          [Field.string("id", paths: ["$.trace_id", "$.traceId"])],
          %{"trace_id" => "first", "traceId" => "second"}
        )

      assert result["id"] == "first"
    end

    test "coalesce string: empty strings skipped" do
      result =
        compile_and_map(
          [Field.string("level", paths: ["$.level", "$.metadata.level"], default: "INFO")],
          %{"level" => "", "metadata" => %{"level" => "debug"}}
        )

      assert result["level"] == "debug"
    end

    test "deep nested path" do
      result =
        compile_and_map(
          [Field.string("app", path: "$.metadata.context.application")],
          %{"metadata" => %{"context" => %{"application" => "myapp"}}}
        )

      assert result["app"] == "myapp"
    end

    test "missing path returns default" do
      result =
        compile_and_map(
          [Field.string("missing", path: "$.not_here", default: "fallback")],
          %{"other" => "value"}
        )

      assert result["missing"] == "fallback"
    end

    test "root path returns entire document" do
      doc = %{"a" => 1, "b" => 2}

      result =
        compile_and_map(
          [Field.json("body", path: "$")],
          doc
        )

      assert result["body"] == doc
    end
  end

  # ── Type coercion ─────────────────────────────────────────────────────

  describe "type coercion: string" do
    test "pass-through binary" do
      result =
        compile_and_map(
          [Field.string("val", path: "$.val")],
          %{"val" => "hello"}
        )

      assert result["val"] == "hello"
    end

    test "integer to string" do
      result =
        compile_and_map(
          [Field.string("val", path: "$.val")],
          %{"val" => 42}
        )

      assert result["val"] == "42"
    end

    test "nil to default" do
      result =
        compile_and_map(
          [Field.string("val", path: "$.val", default: "none")],
          %{}
        )

      assert result["val"] == "none"
    end
  end

  describe "type coercion: integers" do
    test "uint8 clamping" do
      result =
        compile_and_map(
          [Field.uint8("val", path: "$.val")],
          %{"val" => 300}
        )

      assert result["val"] == 255
    end

    test "uint8 negative clamped to 0" do
      result =
        compile_and_map(
          [Field.uint8("val", path: "$.val")],
          %{"val" => -5}
        )

      assert result["val"] == 0
    end

    test "uint32 normal value" do
      result =
        compile_and_map(
          [Field.uint32("val", path: "$.val")],
          %{"val" => 1000}
        )

      assert result["val"] == 1000
    end

    test "uint64 large value" do
      result =
        compile_and_map(
          [Field.uint64("val", path: "$.val")],
          %{"val" => 18_446_744_073_709_551}
        )

      assert result["val"] == 18_446_744_073_709_551
    end

    test "int32 signed" do
      result =
        compile_and_map(
          [Field.int32("val", path: "$.val")],
          %{"val" => -42}
        )

      assert result["val"] == -42
    end

    test "int32 clamping" do
      result =
        compile_and_map(
          [Field.int32("val", path: "$.val")],
          %{"val" => 3_000_000_000}
        )

      assert result["val"] == 2_147_483_647
    end

    test "float truncation to uint" do
      result =
        compile_and_map(
          [Field.uint32("val", path: "$.val")],
          %{"val" => 42.9}
        )

      assert result["val"] == 42
    end
  end

  describe "type coercion: float64" do
    test "float pass-through" do
      result =
        compile_and_map(
          [Field.float64("val", path: "$.val")],
          %{"val" => 3.14}
        )

      assert_in_delta result["val"], 3.14, 0.001
    end

    test "integer to float" do
      result =
        compile_and_map(
          [Field.float64("val", path: "$.val")],
          %{"val" => 42}
        )

      assert result["val"] == 42.0
    end
  end

  describe "type coercion: bool" do
    test "true/false pass-through" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => true}
        )

      assert result["val"] == true
    end

    test "truthy string coercion" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => "true"}
        )

      assert result["val"] == true
    end

    test "truthy 1 string" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => "1"}
        )

      assert result["val"] == true
    end

    test "falsy string" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => "false"}
        )

      assert result["val"] == false
    end

    test "integer truthy" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => 1}
        )

      assert result["val"] == true
    end

    test "integer falsy" do
      result =
        compile_and_map(
          [Field.bool("val", path: "$.val")],
          %{"val" => 0}
        )

      assert result["val"] == false
    end
  end

  # ── DateTime64 normalization ──────────────────────────────────────────

  describe "DateTime64 normalization" do
    test "seconds precision scaled to nanoseconds" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => 1_769_018_088}
        )

      assert result["ts"] == 1_769_018_088_000_000_000
    end

    test "milliseconds scaled to nanoseconds" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => 1_769_018_088_144}
        )

      assert result["ts"] == 1_769_018_088_144_000_000
    end

    test "microseconds scaled to nanoseconds" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => 1_769_018_088_144_506}
        )

      assert result["ts"] == 1_769_018_088_144_506_000
    end

    test "nanoseconds pass-through" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => 1_769_018_088_144_506_000}
        )

      assert result["ts"] == 1_769_018_088_144_506_000
    end

    test "ISO8601 string parsed" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => "2026-01-21T17:54:48.144506Z"}
        )

      assert is_integer(result["ts"])
      assert result["ts"] > 0
    end

    test "RFC3339 with timezone offset" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => "2025-12-30T16:21:38+00:00"}
        )

      assert is_integer(result["ts"])
      assert result["ts"] > 0
    end

    test "invalid returns 0" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => "not-a-date"}
        )

      assert result["ts"] == 0
    end

    test "missing returns nil (not epoch zero)" do
      result =
        compile_and_map(
          [Field.datetime64("ts", path: "$.ts", precision: 9)],
          %{}
        )

      assert is_nil(result["ts"])
    end
  end

  # ── JSON columns ──────────────────────────────────────────────────────

  describe "JSON columns" do
    test "sub-tree extraction returns map" do
      doc = %{"resource" => %{"service" => %{"name" => "web"}}}

      result =
        compile_and_map(
          [Field.json("attrs", path: "$.resource")],
          doc
        )

      assert result["attrs"] == %{"service" => %{"name" => "web"}}
    end

    test "exclude_keys removes specified keys" do
      doc = %{"id" => "uuid", "event_message" => "hello", "timestamp" => 123, "level" => "info"}

      result =
        compile_and_map(
          [Field.json("attrs", path: "$", exclude_keys: ["id", "timestamp"])],
          doc
        )

      assert result["attrs"] == %{"event_message" => "hello", "level" => "info"}
    end

    test "elevate_keys merges children into parent" do
      doc = %{
        "level" => "info",
        "metadata" => %{"region" => "us-east", "host" => "web1"}
      }

      result =
        compile_and_map(
          [Field.json("attrs", path: "$", elevate_keys: ["metadata"])],
          doc
        )

      assert result["attrs"]["level"] == "info"
      assert result["attrs"]["region"] == "us-east"
      assert result["attrs"]["host"] == "web1"
      refute Map.has_key?(result["attrs"], "metadata")
    end

    test "elevate_keys: existing top-level wins over elevated children" do
      doc = %{
        "level" => "info",
        "metadata" => %{"level" => "debug", "extra" => "data"}
      }

      result =
        compile_and_map(
          [Field.json("attrs", path: "$", elevate_keys: ["metadata"])],
          doc
        )

      assert result["attrs"]["level"] == "info"
      assert result["attrs"]["extra"] == "data"
    end

    test "combined exclude + elevate on root path" do
      doc = %{
        "id" => "uuid",
        "event_message" => "hello",
        "timestamp" => 123,
        "metadata" => %{"region" => "us-east"}
      }

      result =
        compile_and_map(
          [
            Field.json("attrs",
              path: "$",
              exclude_keys: ["id", "event_message", "timestamp"],
              elevate_keys: ["metadata"]
            )
          ],
          doc
        )

      assert result["attrs"] == %{"region" => "us-east"}
    end
  end

  # ── Pick ──────────────────────────────────────────────────────────────

  describe "pick" do
    test "sparse map assembly with resolved paths" do
      doc = %{
        "metadata" => %{"region" => "eu-west-2"},
        "cluster" => "prod-1"
      }

      result =
        compile_and_map(
          [
            Field.json("attrs",
              paths: ["$.resource"],
              pick: [
                {"region", ["$.metadata.region", "$.region"]},
                {"cluster", ["$.cluster"]}
              ]
            )
          ],
          doc
        )

      assert result["attrs"] == %{"region" => "eu-west-2", "cluster" => "prod-1"}
    end

    test "unresolved paths omitted (sparse)" do
      doc = %{"region" => "us-west"}

      result =
        compile_and_map(
          [
            Field.json("attrs",
              paths: ["$.resource"],
              pick: [
                {"region", ["$.region"]},
                {"cluster", ["$.cluster"]}
              ]
            )
          ],
          doc
        )

      assert result["attrs"] == %{"region" => "us-west"}
      refute Map.has_key?(result["attrs"], "cluster")
    end

    test "pick-first: non-empty pick used as field value" do
      doc = %{
        "region" => "us-east",
        "resource" => %{"service" => %{"name" => "web"}}
      }

      result =
        compile_and_map(
          [
            Field.json("attrs",
              paths: ["$.resource"],
              pick: [{"region", ["$.region"]}]
            )
          ],
          doc
        )

      # Pick produced a result, so use it instead of $.resource
      assert result["attrs"] == %{"region" => "us-east"}
    end

    test "path-fallback: empty pick falls back to paths" do
      doc = %{
        "resource" => %{"service" => %{"name" => "web"}}
      }

      result =
        compile_and_map(
          [
            Field.json("attrs",
              paths: ["$.resource"],
              pick: [
                {"region", ["$.metadata.region"]},
                {"cluster", ["$.metadata.cluster"]}
              ]
            )
          ],
          doc
        )

      # Pick produced nothing, fall back to $.resource
      assert result["attrs"] == %{"service" => %{"name" => "web"}}
    end

    test "pick with coalesce paths per entry" do
      doc = %{
        "metadata" => %{"svc" => "api"},
        "resource" => %{"service" => %{"name" => "web"}}
      }

      result =
        compile_and_map(
          [
            Field.json("attrs",
              paths: ["$.resource"],
              pick: [
                {"service_name", ["$.resource.service.name", "$.metadata.svc"]}
              ]
            )
          ],
          doc
        )

      assert result["attrs"] == %{"service_name" => "web"}
    end
  end

  # ── Enum8 ─────────────────────────────────────────────────────────────

  describe "enum8" do
    test "explicit path lookup -> string matched -> integer" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "sum" => 2, "histogram" => 3},
              default: 1
            )
          ],
          %{"metric_type" => "histogram"}
        )

      assert result["mt"] == 3
    end

    test "case-insensitive string matching" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "sum" => 2},
              default: 1
            )
          ],
          %{"metric_type" => "GAUGE"}
        )

      assert result["mt"] == 1
    end

    test "no match returns default" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "sum" => 2},
              default: 1
            )
          ],
          %{}
        )

      assert result["mt"] == 1
    end

    test "structural inference: any (OR) conditions" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "histogram" => 3},
              infer: [
                %InferRule{
                  result: "histogram",
                  any: [
                    %InferCondition{path: "$.bucket_counts", predicate: "not_empty"},
                    %InferCondition{path: "$.explicit_bounds", predicate: "not_empty"}
                  ],
                  all: []
                }
              ],
              default: 1
            )
          ],
          %{"bucket_counts" => [1, 2, 3]}
        )

      assert result["mt"] == 3
    end

    test "structural inference: all (AND) conditions" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "histogram" => 3},
              infer: [
                %InferRule{
                  result: "histogram",
                  any: [],
                  all: [
                    %InferCondition{path: "$.count", predicate: "not_zero"},
                    %InferCondition{path: "$.sum", predicate: "not_zero"}
                  ]
                }
              ],
              default: 1
            )
          ],
          %{"count" => 10, "sum" => 42.5}
        )

      assert result["mt"] == 3
    end

    test "structural inference: all conditions not met returns default" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "histogram" => 3},
              infer: [
                %InferRule{
                  result: "histogram",
                  any: [],
                  all: [
                    %InferCondition{path: "$.count", predicate: "not_zero"},
                    %InferCondition{path: "$.sum", predicate: "not_zero"}
                  ]
                }
              ],
              default: 1
            )
          ],
          %{"count" => 10, "sum" => 0}
        )

      assert result["mt"] == 1
    end

    test "combined any + all on same rule" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "histogram" => 3},
              infer: [
                %InferRule{
                  result: "histogram",
                  any: [
                    %InferCondition{path: "$.bucket_counts", predicate: "not_empty"}
                  ],
                  all: [
                    %InferCondition{path: "$.count", predicate: "not_zero"}
                  ]
                }
              ],
              default: 1
            )
          ],
          %{"bucket_counts" => [1, 2], "count" => 5}
        )

      assert result["mt"] == 3
    end

    test "multiple rules: first match wins" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"gauge" => 1, "sum" => 2, "histogram" => 3},
              infer: [
                %InferRule{
                  result: "sum",
                  any: [%InferCondition{path: "$.is_monotonic", predicate: "exists"}],
                  all: []
                },
                %InferRule{
                  result: "histogram",
                  any: [%InferCondition{path: "$.bucket_counts", predicate: "not_empty"}],
                  all: []
                }
              ],
              default: 1
            )
          ],
          # Both could match, but "sum" rule is first
          %{"is_monotonic" => true, "bucket_counts" => [1, 2]}
        )

      assert result["mt"] == 2
    end
  end

  describe "predicates" do
    test "exists" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"found" => 1},
              infer: [
                %InferRule{
                  result: "found",
                  any: [%InferCondition{path: "$.marker", predicate: "exists"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"marker" => "present"}
        )

      assert result["mt"] == 1
    end

    test "not_exists" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"missing" => 1},
              infer: [
                %InferRule{
                  result: "missing",
                  any: [%InferCondition{path: "$.marker", predicate: "not_exists"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"other" => "value"}
        )

      assert result["mt"] == 1
    end

    test "not_zero with integer" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"active" => 1},
              infer: [
                %InferRule{
                  result: "active",
                  any: [%InferCondition{path: "$.count", predicate: "not_zero"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"count" => 42}
        )

      assert result["mt"] == 1
    end

    test "is_zero" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"zero" => 1},
              infer: [
                %InferRule{
                  result: "zero",
                  any: [%InferCondition{path: "$.value", predicate: "is_zero"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"value" => 0}
        )

      assert result["mt"] == 1
    end

    test "not_empty with list" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"has_data" => 1},
              infer: [
                %InferRule{
                  result: "has_data",
                  any: [%InferCondition{path: "$.items", predicate: "not_empty"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"items" => [1, 2, 3]}
        )

      assert result["mt"] == 1
    end

    test "is_empty with empty list" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"empty" => 1},
              infer: [
                %InferRule{
                  result: "empty",
                  any: [%InferCondition{path: "$.items", predicate: "is_empty"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"items" => []}
        )

      assert result["mt"] == 1
    end

    test "is_string" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"str" => 1},
              infer: [
                %InferRule{
                  result: "str",
                  any: [%InferCondition{path: "$.val", predicate: "is_string"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"val" => "hello"}
        )

      assert result["mt"] == 1
    end

    test "is_number" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"num" => 1},
              infer: [
                %InferRule{
                  result: "num",
                  any: [%InferCondition{path: "$.val", predicate: "is_number"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"val" => 42}
        )

      assert result["mt"] == 1
    end

    test "is_list" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"list" => 1},
              infer: [
                %InferRule{
                  result: "list",
                  any: [%InferCondition{path: "$.val", predicate: "is_list"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"val" => [1, 2]}
        )

      assert result["mt"] == 1
    end

    test "is_map" do
      result =
        compile_and_map(
          [
            Field.enum8("mt",
              paths: ["$.metric_type"],
              values: %{"map" => 1},
              infer: [
                %InferRule{
                  result: "map",
                  any: [%InferCondition{path: "$.val", predicate: "is_map"}],
                  all: []
                }
              ],
              default: 0
            )
          ],
          %{"val" => %{"a" => 1}}
        )

      assert result["mt"] == 1
    end
  end

  # ── FromOutput ────────────────────────────────────────────────────────

  describe "FromOutput" do
    test "derived field reads from already-resolved output" do
      result =
        compile_and_map(
          [
            Field.string("severity_text",
              paths: ["$.level"],
              default: "INFO",
              transform: "upcase"
            ),
            Field.uint8("severity_number",
              from_output: "severity_text",
              value_map: %{
                "TRACE" => 1,
                "DEBUG" => 5,
                "INFO" => 9,
                "WARN" => 13,
                "ERROR" => 17,
                "FATAL" => 21
              },
              default: 0
            )
          ],
          %{"level" => "error"}
        )

      assert result["severity_text"] == "ERROR"
      assert result["severity_number"] == 17
    end

    test "FromOutput with missing source field uses default" do
      result =
        compile_and_map(
          [
            Field.string("severity_text",
              paths: ["$.level"],
              default: "INFO",
              transform: "upcase"
            ),
            Field.uint8("severity_number",
              from_output: "severity_text",
              value_map: %{"INFO" => 9, "ERROR" => 17},
              default: 0
            )
          ],
          %{}
        )

      assert result["severity_text"] == "INFO"
      assert result["severity_number"] == 9
    end

    test "FromOutput with value not in value_map uses default" do
      result =
        compile_and_map(
          [
            Field.string("severity_text",
              paths: ["$.level"],
              default: "",
              transform: "upcase"
            ),
            Field.uint8("severity_number",
              from_output: "severity_text",
              value_map: %{"INFO" => 9, "ERROR" => 17},
              default: 0
            )
          ],
          %{"level" => "custom_level"}
        )

      assert result["severity_text"] == "CUSTOM_LEVEL"
      assert result["severity_number"] == 0
    end
  end

  # ── Transforms ────────────────────────────────────────────────────────

  describe "transforms" do
    test "upcase" do
      result =
        compile_and_map(
          [Field.string("val", path: "$.val", transform: "upcase")],
          %{"val" => "hello"}
        )

      assert result["val"] == "HELLO"
    end

    test "downcase" do
      result =
        compile_and_map(
          [Field.string("val", path: "$.val", transform: "downcase")],
          %{"val" => "HELLO"}
        )

      assert result["val"] == "hello"
    end
  end

  # ── Allowed values ──────────────────────────────────────────────────

  describe "allowed_values" do
    test "value in set passes through" do
      result =
        compile_and_map(
          [
            Field.string("level",
              path: "$.level",
              default: "INFO",
              allowed_values: ~w(INFO ERROR WARN)
            )
          ],
          %{"level" => "ERROR"}
        )

      assert result["level"] == "ERROR"
    end

    test "value not in set falls back to default" do
      result =
        compile_and_map(
          [
            Field.string("level",
              path: "$.level",
              default: "INFO",
              allowed_values: ~w(INFO ERROR WARN)
            )
          ],
          %{"level" => "some random stack trace string"}
        )

      assert result["level"] == "INFO"
    end

    test "missing path (nil) uses default without checking allowed_values" do
      result =
        compile_and_map(
          [
            Field.string("level",
              path: "$.level",
              default: "INFO",
              allowed_values: ~w(INFO ERROR WARN)
            )
          ],
          %{}
        )

      assert result["level"] == "INFO"
    end

    test "empty allowed_values means no filtering" do
      result =
        compile_and_map(
          [Field.string("level", path: "$.level", default: "INFO")],
          %{"level" => "any_random_value"}
        )

      assert result["level"] == "any_random_value"
    end

    test "interaction with value_map + from_output: invalid falls back to default then maps correctly" do
      result =
        compile_and_map(
          [
            Field.string("severity_text",
              path: "$.level",
              default: "INFO",
              transform: "upcase",
              allowed_values: ~w(INFO ERROR WARN)
            ),
            Field.uint8("severity_number",
              from_output: "severity_text",
              value_map: %{"INFO" => 9, "ERROR" => 17, "WARN" => 13},
              default: 0
            )
          ],
          %{"level" => "java.lang.NullPointerException\n  at com.foo.Bar"}
        )

      # Invalid severity_text falls back to default "INFO"
      assert result["severity_text"] == "INFO"
      # severity_number correctly maps the default
      assert result["severity_number"] == 9
    end

    test "transform runs before allowed_values check" do
      result =
        compile_and_map(
          [
            Field.string("level",
              path: "$.level",
              default: "INFO",
              transform: "upcase",
              allowed_values: ~w(INFO ERROR WARN)
            )
          ],
          # lowercase input + upcase transform = "INFO" which is in allowed_values
          %{"level" => "info"}
        )

      assert result["level"] == "INFO"
    end

    test "non-string value falls back to default" do
      result =
        compile_and_map(
          [
            Field.string("val",
              path: "$.val",
              default: "fallback",
              allowed_values: ~w(hello world)
            )
          ],
          # Integer input is not in allowed_values, so falls back to default
          %{"val" => 42}
        )

      assert result["val"] == "fallback"
    end
  end

  # ── Error handling ────────────────────────────────────────────────────

  describe "run/2" do
    test "compiles and maps in one step" do
      config = MappingConfig.new([Field.string("name", path: "$.name")])
      assert {:ok, %{"name" => "Alice"}} = Mapper.run(%{"name" => "Alice"}, config)
    end

    test "returns error tuple on invalid config" do
      config =
        MappingConfig.new([
          %Logflare.Mapper.MappingConfig.FieldConfig{name: "val", type: "unknown_type"}
        ])

      assert {:error, reason} = Mapper.run(%{"val" => 1}, config)
      assert reason =~ "unknown field type"
    end
  end

  describe "compile/1" do
    test "returns {:ok, reference} on valid config" do
      config = MappingConfig.new([Field.string("name", path: "$.name")])
      assert {:ok, compiled} = Mapper.compile(config)
      assert is_reference(compiled)
    end

    test "returns {:error, reason} on invalid config" do
      config =
        MappingConfig.new([
          %Logflare.Mapper.MappingConfig.FieldConfig{name: "val", type: "unknown_type"}
        ])

      assert {:error, reason} = Mapper.compile(config)
      assert reason =~ "unknown field type"
    end

    test "returns {:error, reason} on duplicate field names" do
      config =
        MappingConfig.new([
          Field.string("name", path: "$.first_name"),
          Field.string("name", path: "$.last_name")
        ])

      assert {:error, reason} = Mapper.compile(config)
      assert reason =~ "duplicate field name: 'name'"
    end
  end

  describe "error handling" do
    test "compile! raises on unknown field type" do
      assert_raise ArgumentError, ~r/unknown field type/, fn ->
        config =
          MappingConfig.new([
            %Logflare.Mapper.MappingConfig.FieldConfig{name: "val", type: "unknown_type"}
          ])

        Mapper.compile!(config)
      end
    end

    test "compile! raises on invalid path syntax" do
      assert_raise ArgumentError, ~r/path must start with/, fn ->
        config =
          MappingConfig.new([
            Field.string("val", path: "no_dollar")
          ])

        Mapper.compile!(config)
      end
    end
  end

  # ── Array types ───────────────────────────────────────────────────────

  describe "array_string" do
    test "extracts list of strings" do
      result =
        compile_and_map(
          [Field.array_string("tags", path: "$.tags")],
          %{"tags" => ["web", "production", "us-east"]}
        )

      assert result["tags"] == ["web", "production", "us-east"]
    end

    test "coerces non-string elements to strings" do
      result =
        compile_and_map(
          [Field.array_string("vals", path: "$.vals")],
          %{"vals" => [42, 3.14, true, "hello"]}
        )

      assert result["vals"] == ["42", "3.14", "true", "hello"]
    end

    test "nil elements coerced to empty string by default" do
      result =
        compile_and_map(
          [Field.array_string("tags", path: "$.tags")],
          %{"tags" => ["a", nil, "b"]}
        )

      assert result["tags"] == ["a", "", "b"]
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_string("tags", path: "$.tags", filter_nil: true)],
          %{"tags" => ["a", nil, "b"]}
        )

      assert result["tags"] == ["a", "b"]
    end

    test "missing path returns empty list" do
      result =
        compile_and_map(
          [Field.array_string("tags", path: "$.tags")],
          %{}
        )

      assert result["tags"] == []
    end

    test "non-list value returns empty list" do
      result =
        compile_and_map(
          [Field.array_string("tags", path: "$.tags")],
          %{"tags" => "not_a_list"}
        )

      assert result["tags"] == []
    end

    test "wildcard extracts field from each object in a list" do
      result =
        compile_and_map(
          [Field.array_string("names", path: "$.people[*].name")],
          %{
            "people" => [
              %{"name" => "Bob", "age" => 50},
              %{"name" => "Fred", "age" => 64}
            ]
          }
        )

      assert result["names"] == ["Bob", "Fred"]
    end

    test "wildcard with nested path" do
      result =
        compile_and_map(
          [Field.array_string("cities", path: "$.users[*].address.city")],
          %{
            "users" => [
              %{"address" => %{"city" => "NYC"}},
              %{"address" => %{"city" => "LA"}},
              %{"address" => %{"city" => "Chicago"}}
            ]
          }
        )

      assert result["cities"] == ["NYC", "LA", "Chicago"]
    end

    test "wildcard coerces non-string values to strings" do
      result =
        compile_and_map(
          [Field.array_string("ids", path: "$.items[*].id")],
          %{
            "items" => [
              %{"id" => 101},
              %{"id" => 202},
              %{"id" => 303}
            ]
          }
        )

      assert result["ids"] == ["101", "202", "303"]
    end
  end

  describe "array_uint64" do
    test "extracts list of unsigned integers" do
      result =
        compile_and_map(
          [Field.array_uint64("counts", path: "$.counts")],
          %{"counts" => [0, 5, 10, 100]}
        )

      assert result["counts"] == [0, 5, 10, 100]
    end

    test "negative values clamped to 0" do
      result =
        compile_and_map(
          [Field.array_uint64("vals", path: "$.vals")],
          %{"vals" => [-5, 10, -1]}
        )

      assert result["vals"] == [0, 10, 0]
    end

    test "nil elements coerced to 0 by default" do
      result =
        compile_and_map(
          [Field.array_uint64("counts", path: "$.counts")],
          %{"counts" => [1, nil, 3]}
        )

      assert result["counts"] == [1, 0, 3]
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_uint64("counts", path: "$.counts", filter_nil: true)],
          %{"counts" => [1, nil, 3]}
        )

      assert result["counts"] == [1, 3]
    end

    test "float values truncated to integers" do
      result =
        compile_and_map(
          [Field.array_uint64("vals", path: "$.vals")],
          %{"vals" => [1.9, 2.1, 3.5]}
        )

      assert result["vals"] == [1, 2, 3]
    end

    test "wildcard extracts numeric field from each object" do
      result =
        compile_and_map(
          [Field.array_uint64("ages", path: "$.people[*].age")],
          %{
            "people" => [
              %{"name" => "Bob", "age" => 50},
              %{"name" => "Fred", "age" => 64}
            ]
          }
        )

      assert result["ages"] == [50, 64]
    end
  end

  describe "array_float64" do
    test "extracts list of floats" do
      result =
        compile_and_map(
          [Field.array_float64("bounds", path: "$.bounds")],
          %{"bounds" => [0.0, 5.0, 10.0, 25.0, 50.0]}
        )

      assert result["bounds"] == [0.0, 5.0, 10.0, 25.0, 50.0]
    end

    test "integers coerced to floats" do
      result =
        compile_and_map(
          [Field.array_float64("vals", path: "$.vals")],
          %{"vals" => [1, 2, 3]}
        )

      assert result["vals"] == [1.0, 2.0, 3.0]
    end

    test "string numbers parsed" do
      result =
        compile_and_map(
          [Field.array_float64("vals", path: "$.vals")],
          %{"vals" => ["3.14", "2.72"]}
        )

      assert_in_delta Enum.at(result["vals"], 0), 3.14, 0.001
      assert_in_delta Enum.at(result["vals"], 1), 2.72, 0.001
    end

    test "nil elements coerced to 0.0 by default" do
      result =
        compile_and_map(
          [Field.array_float64("bounds", path: "$.bounds")],
          %{"bounds" => [1.0, nil, 3.0]}
        )

      assert result["bounds"] == [1.0, 0.0, 3.0]
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_float64("bounds", path: "$.bounds", filter_nil: true)],
          %{"bounds" => [1.0, nil, 3.0]}
        )

      assert result["bounds"] == [1.0, 3.0]
    end

    test "wildcard extracts float field from each object" do
      result =
        compile_and_map(
          [Field.array_float64("scores", path: "$.results[*].score")],
          %{
            "results" => [
              %{"label" => "a", "score" => 0.95},
              %{"label" => "b", "score" => 0.82},
              %{"label" => "c", "score" => 0.71}
            ]
          }
        )

      assert result["scores"] == [0.95, 0.82, 0.71]
    end
  end

  describe "array_datetime64" do
    test "scales integer timestamps to target precision" do
      result =
        compile_and_map(
          [Field.array_datetime64("timestamps", path: "$.ts", precision: 9)],
          %{"ts" => [1_769_018_088, 1_769_018_088_144]}
        )

      # seconds -> nanoseconds, milliseconds -> nanoseconds
      assert Enum.at(result["timestamps"], 0) == 1_769_018_088_000_000_000
      assert Enum.at(result["timestamps"], 1) == 1_769_018_088_144_000_000
    end

    test "nil elements coerced to 0 (epoch) by default" do
      result =
        compile_and_map(
          [Field.array_datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => [1_769_018_088, nil]}
        )

      assert Enum.at(result["ts"], 0) == 1_769_018_088_000_000_000
      assert Enum.at(result["ts"], 1) == 0
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_datetime64("ts", path: "$.ts", precision: 9, filter_nil: true)],
          %{"ts" => [1_769_018_088, nil]}
        )

      assert result["ts"] == [1_769_018_088_000_000_000]
    end

    test "ISO8601 strings parsed in array" do
      result =
        compile_and_map(
          [Field.array_datetime64("ts", path: "$.ts", precision: 9)],
          %{"ts" => ["2026-01-21T17:54:48.144506Z"]}
        )

      assert is_integer(Enum.at(result["ts"], 0))
      assert Enum.at(result["ts"], 0) > 0
    end
  end

  describe "array_json" do
    test "pass-through of mixed-type list elements" do
      result =
        compile_and_map(
          [Field.array_json("events", path: "$.events")],
          %{"events" => [%{"name" => "click"}, %{"name" => "view"}, "plain_string", 42]}
        )

      assert result["events"] == [%{"name" => "click"}, %{"name" => "view"}, "plain_string", 42]
    end

    test "nil elements coerced to empty map by default" do
      result =
        compile_and_map(
          [Field.array_json("items", path: "$.items")],
          %{"items" => [%{"a" => 1}, nil, %{"b" => 2}]}
        )

      assert result["items"] == [%{"a" => 1}, %{}, %{"b" => 2}]
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_json("items", path: "$.items", filter_nil: true)],
          %{"items" => [%{"a" => 1}, nil, %{"b" => 2}]}
        )

      assert result["items"] == [%{"a" => 1}, %{"b" => 2}]
    end

    test "missing path returns empty list" do
      result =
        compile_and_map(
          [Field.array_json("events", path: "$.events")],
          %{}
        )

      assert result["events"] == []
    end
  end

  describe "array_map" do
    test "extracts list of maps" do
      result =
        compile_and_map(
          [Field.array_map("links", path: "$.links")],
          %{"links" => [%{"url" => "http://a"}, %{"url" => "http://b"}]}
        )

      assert result["links"] == [%{"url" => "http://a"}, %{"url" => "http://b"}]
    end

    test "non-map elements always filtered out" do
      result =
        compile_and_map(
          [Field.array_map("links", path: "$.links")],
          %{"links" => [%{"url" => "http://a"}, "invalid", 42, %{"url" => "http://b"}]}
        )

      assert result["links"] == [%{"url" => "http://a"}, %{"url" => "http://b"}]
    end

    test "nil elements coerced to empty map by default" do
      result =
        compile_and_map(
          [Field.array_map("links", path: "$.links")],
          %{"links" => [%{"url" => "http://a"}, nil]}
        )

      assert result["links"] == [%{"url" => "http://a"}, %{}]
    end

    test "nil elements filtered with filter_nil: true" do
      result =
        compile_and_map(
          [Field.array_map("links", path: "$.links", filter_nil: true)],
          %{"links" => [%{"url" => "http://a"}, nil]}
        )

      assert result["links"] == [%{"url" => "http://a"}]
    end

    test "missing path returns empty list" do
      result =
        compile_and_map(
          [Field.array_map("links", path: "$.links")],
          %{}
        )

      assert result["links"] == []
    end

    test "wildcard extracts sub-maps from each object" do
      result =
        compile_and_map(
          [Field.array_map("attrs", path: "$.events[*].attributes")],
          %{
            "events" => [
              %{"name" => "click", "attributes" => %{"button" => "submit"}},
              %{"name" => "view", "attributes" => %{"page" => "/home"}}
            ]
          }
        )

      assert result["attrs"] == [
               %{"button" => "submit"},
               %{"page" => "/home"}
             ]
    end
  end

  describe "array type coalesce paths" do
    test "first non-nil list wins" do
      result =
        compile_and_map(
          [Field.array_float64("bounds", paths: ["$.explicit_bounds", "$.bounds"])],
          %{"bounds" => [1.0, 2.0, 3.0]}
        )

      assert result["bounds"] == [1.0, 2.0, 3.0]
    end
  end

  describe "array type empty list" do
    test "empty list passes through" do
      result =
        compile_and_map(
          [Field.array_uint64("counts", path: "$.counts")],
          %{"counts" => []}
        )

      assert result["counts"] == []
    end
  end

  # ── Multi-field integration test ──────────────────────────────────────

  describe "integration" do
    test "log-like document with multiple field types" do
      doc = %{
        "event_message" => "User logged in",
        "timestamp" => 1_769_018_088_144_506,
        "trace_id" => "abc123def456",
        "level" => "info",
        "id" => "some-uuid",
        "metadata" => %{
          "region" => "us-east-1",
          "host" => "web-01",
          "level" => "debug"
        }
      }

      result =
        compile_and_map(
          [
            Field.string("event_message", paths: ["$.event_message", "$.message"]),
            Field.string("trace_id",
              paths: ["$.trace_id", "$.traceId"],
              default: ""
            ),
            Field.string("severity_text",
              paths: ["$.severity_text", "$.level", "$.metadata.level"],
              default: "INFO",
              transform: "upcase"
            ),
            Field.uint8("severity_number",
              from_output: "severity_text",
              value_map: %{
                "TRACE" => 1,
                "DEBUG" => 5,
                "INFO" => 9,
                "WARN" => 13,
                "ERROR" => 17,
                "FATAL" => 21
              },
              default: 0
            ),
            Field.datetime64("timestamp", path: "$.timestamp", precision: 9),
            Field.json("log_attributes",
              path: "$",
              exclude_keys: ["id", "event_message", "timestamp"],
              elevate_keys: ["metadata"]
            )
          ],
          doc
        )

      assert result["event_message"] == "User logged in"
      assert result["trace_id"] == "abc123def456"
      assert result["severity_text"] == "INFO"
      assert result["severity_number"] == 9
      assert result["timestamp"] == 1_769_018_088_144_506_000

      # log_attributes: root with exclude + elevate
      attrs = result["log_attributes"]
      refute Map.has_key?(attrs, "id")
      refute Map.has_key?(attrs, "event_message")
      refute Map.has_key?(attrs, "timestamp")
      refute Map.has_key?(attrs, "metadata")
      assert attrs["region"] == "us-east-1"
      assert attrs["host"] == "web-01"
      # top-level "level" wins over elevated metadata.level
      assert attrs["level"] == "info"
    end
  end

  # ── FlatMap type ───────────────────────────────────────────────────────

  describe "flat_map type" do
    test "flattens a nested map to dot-notation keys with string values" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{"attributes" => %{"a" => %{"b" => %{"c" => 1}}}}
        )

      assert result["attrs"] == %{"a.b.c" => "1"}
    end

    test "coerces scalar values to strings" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{
            "attributes" => %{
              "int_val" => 42,
              "float_val" => 3.14,
              "bool_true" => true,
              "bool_false" => false,
              "string_val" => "hello"
            }
          }
        )

      attrs = result["attrs"]
      assert attrs["int_val"] == "42"
      assert attrs["float_val"] == "3.14"
      assert attrs["bool_true"] == "true"
      assert attrs["bool_false"] == "false"
      assert attrs["string_val"] == "hello"
    end

    test "JSON-encodes list values" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{"attributes" => %{"tags" => [1, 2, 3]}}
        )

      assert result["attrs"]["tags"] == "[1,2,3]"
    end

    test "omits nil values" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{"attributes" => %{"present" => "yes", "absent" => nil}}
        )

      assert result["attrs"] == %{"present" => "yes"}
    end

    test "returns empty map for nil input" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{}
        )

      assert result["attrs"] == %{}
    end

    test "returns empty map for non-map input" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.value")],
          %{"value" => "just a string"}
        )

      assert result["attrs"] == %{}
    end

    test "flattens deeply nested structures" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{
            "attributes" => %{
              "level1" => %{
                "level2" => %{
                  "level3" => "deep"
                }
              }
            }
          }
        )

      assert result["attrs"] == %{"level1.level2.level3" => "deep"}
    end

    test "handles mixed nested and flat keys" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{
            "attributes" => %{
              "simple" => "val",
              "nested" => %{"key" => "val2"}
            }
          }
        )

      attrs = result["attrs"]
      assert attrs["simple"] == "val"
      assert attrs["nested.key"] == "val2"
    end

    test "handles empty nested map" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{"attributes" => %{"empty_map" => %{}}}
        )

      assert result["attrs"] == %{"empty_map" => "{}"}
    end

    test "supports pick entries" do
      result =
        compile_and_map(
          [
            Field.flat_map("resource_attributes",
              paths: ["$.resource"],
              pick: [
                {"service_name", ["$.resource.service.name", "$.service_name"]},
                {"region", ["$.metadata.region"]}
              ]
            )
          ],
          %{
            "service_name" => "my-service",
            "metadata" => %{"region" => "us-east-1"}
          }
        )

      attrs = result["resource_attributes"]
      assert attrs["service_name"] == "my-service"
      assert attrs["region"] == "us-east-1"
    end

    test "supports exclude_keys" do
      result =
        compile_and_map(
          [
            Field.flat_map("attrs",
              path: "$",
              exclude_keys: ["id", "timestamp"]
            )
          ],
          %{"id" => "123", "timestamp" => 999, "name" => "test"}
        )

      attrs = result["attrs"]
      refute Map.has_key?(attrs, "id")
      refute Map.has_key?(attrs, "timestamp")
      assert attrs["name"] == "test"
    end

    test "supports elevate_keys" do
      result =
        compile_and_map(
          [
            Field.flat_map("attrs",
              path: "$",
              exclude_keys: ["id"],
              elevate_keys: ["metadata"]
            )
          ],
          %{"id" => "123", "metadata" => %{"level" => "info"}, "extra" => "val"}
        )

      attrs = result["attrs"]
      refute Map.has_key?(attrs, "id")
      assert attrs["level"] == "info"
      assert attrs["extra"] == "val"
    end

    test "JSON-encodes list of mixed types" do
      result =
        compile_and_map(
          [Field.flat_map("attrs", path: "$.attributes")],
          %{"attributes" => %{"mixed" => ["a", 1, true, nil]}}
        )

      assert result["attrs"]["mixed"] == ~s(["a",1,true,null])
    end
  end

  # ── ArrayFlatMap type ──────────────────────────────────────────────────

  describe "array_flat_map type" do
    test "flattens each map element in an array" do
      result =
        compile_and_map(
          [Field.array_flat_map("event_attrs", path: "$.events[*].attributes")],
          %{
            "events" => [
              %{"attributes" => %{"key1" => "val1", "nested" => %{"a" => 1}}},
              %{"attributes" => %{"key2" => "val2"}}
            ]
          }
        )

      assert result["event_attrs"] == [
               %{"key1" => "val1", "nested.a" => "1"},
               %{"key2" => "val2"}
             ]
    end

    test "filters out non-map elements" do
      result =
        compile_and_map(
          [Field.array_flat_map("attrs", path: "$.items")],
          %{"items" => [%{"a" => "1"}, "not a map", 42, %{"b" => "2"}]}
        )

      assert result["attrs"] == [%{"a" => "1"}, %{"b" => "2"}]
    end

    test "defaults to empty list when path is nil" do
      result =
        compile_and_map(
          [Field.array_flat_map("attrs", path: "$.missing")],
          %{}
        )

      assert result["attrs"] == []
    end

    test "handles empty array" do
      result =
        compile_and_map(
          [Field.array_flat_map("attrs", path: "$.items")],
          %{"items" => []}
        )

      assert result["attrs"] == []
    end
  end
end
