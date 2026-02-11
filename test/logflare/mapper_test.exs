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

    test "non-string value bypasses the check and goes to coercion" do
      result =
        compile_and_map(
          [
            Field.string("val",
              path: "$.val",
              default: "fallback",
              allowed_values: ~w(hello world)
            )
          ],
          # Integer input: NIF can't decode as string, so bypasses allowed_values check
          %{"val" => 42}
        )

      # Integer bypasses string check, then gets coerced to string "42"
      assert result["val"] == "42"
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
end
