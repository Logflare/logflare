defmodule Logflare.Mapper.OptimizedMapperTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig
  alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field

  @uint64_max 18_446_744_073_709_551_615
  @shared_keys ~w(a b c d e f g h)

  describe "per-document path caches" do
    test "a compiled mapping isolates cached values across sequential and concurrent calls" do
      fields =
        Enum.map(@shared_keys, fn key ->
          Field.string(key, path: "$.shared.#{key}", default: "missing")
        end) ++
          [Field.string("fallback", paths: ["$.shared.fallback", "$.fallback"], default: "none")]

      compiled = compile(fields)

      cases =
        for index <- 1..120 do
          document = cache_document(index)
          {index, document, expected_cache_result(document)}
        end

      assert Enum.map(cases, fn {_index, document, _expected} ->
               Mapper.map(document, compiled)
             end) ==
               Enum.map(cases, fn {_index, _document, expected} -> expected end)

      results =
        cases
        |> Task.async_stream(
          fn {index, document, expected} ->
            {index, Mapper.map(document, compiled), expected}
          end,
          max_concurrency: 16,
          ordered: false,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, fn {_index, actual, expected} -> actual == expected end)
      assert Enum.sort(Enum.map(results, &elem(&1, 0))) == Enum.to_list(1..120)
    end

    test "repeated missing and nil prefixes coalesce correctly with small and large root maps" do
      fields = [
        Field.string("application",
          paths: [
            "$.metadata.context.primary",
            "$.metadata.context.secondary",
            "$.fallback"
          ],
          default: "unknown"
        ),
        Field.string("version", path: "$.metadata.context.version", default: ""),
        Field.string("region", path: "$.metadata.context.region", default: ""),
        Field.string("node", path: "$.metadata.context.node", default: ""),
        Field.string("environment", path: "$.metadata.context.environment", default: ""),
        Field.string("tenant", path: "$.metadata.context.tenant", default: ""),
        Field.string("release", path: "$.metadata.context.release", default: ""),
        Field.string("cluster", path: "$.metadata.context.cluster", default: "")
      ]

      compiled = compile(fields)

      context = %{
        "primary" => nil,
        "secondary" => "api",
        "version" => "1.2.3",
        "region" => "us-east-1",
        "node" => "node-1",
        "environment" => "prod",
        "tenant" => "tenant-1",
        "release" => "2026-03",
        "cluster" => "cluster-a"
      }

      small = %{"metadata" => %{"context" => context}, "fallback" => "root-fallback"}

      large =
        Enum.reduce(1..200, small, fn index, document ->
          Map.put(document, "unrelated_#{index}", index)
        end)

      expected = %{
        "application" => "api",
        "version" => "1.2.3",
        "region" => "us-east-1",
        "node" => "node-1",
        "environment" => "prod",
        "tenant" => "tenant-1",
        "release" => "2026-03",
        "cluster" => "cluster-a"
      }

      assert Mapper.map(small, compiled) == expected
      assert Mapper.map(large, compiled) == expected

      assert Mapper.map(%{"metadata" => nil, "fallback" => "root-fallback"}, compiled) == %{
               "application" => "root-fallback",
               "version" => "",
               "region" => "",
               "node" => "",
               "environment" => "",
               "tenant" => "",
               "release" => "",
               "cluster" => ""
             }
    end
  end

  describe "single-path wildcard array fast path" do
    test "preserves coercion and nil filtering for every array family" do
      compiled = compile(wildcard_fields())
      timestamp = 1_700_000_000

      document = %{
        "items" => [
          %{
            "name" => "alpha",
            "count" => "42",
            "ratio" => "1.5",
            "time" => timestamp,
            "raw" => [1],
            "payload" => %{"id" => 1},
            "attrs" => %{"nested" => %{"value" => 1}}
          },
          %{
            "name" => nil,
            "count" => nil,
            "ratio" => nil,
            "time" => nil,
            "raw" => nil,
            "payload" => nil,
            "attrs" => nil
          },
          %{},
          "not-a-map",
          %{
            "name" => 123,
            "count" => "invalid",
            "ratio" => "invalid",
            "time" => "invalid",
            "raw" => false,
            "payload" => "invalid",
            "attrs" => ["invalid"]
          }
        ]
      }

      assert Mapper.map(document, compiled) == %{
               "names" => ["alpha", "", "", "", "123"],
               "names_filtered" => ["alpha", "123"],
               "counts" => [42, 0, 0, 0, 0],
               "counts_filtered" => [42, 0],
               "ratios" => [1.5, 0.0, 0.0, 0.0, 0.0],
               "ratios_filtered" => [1.5, 0.0],
               "times" => [timestamp * 1_000_000_000, 0, 0, 0, 0],
               "times_filtered" => [timestamp * 1_000_000_000, 0],
               "raws" => [[1], %{}, %{}, %{}, false],
               "raws_filtered" => [[1], false],
               "payloads" => [%{"id" => 1}, %{}, %{}, %{}],
               "payloads_filtered" => [%{"id" => 1}],
               "flat_attrs" => [%{"nested.value" => "1"}, %{}, %{}, %{}],
               "flat_attrs_filtered" => [%{"nested.value" => "1"}]
             }
    end

    test "returns empty arrays for missing, empty, and non-list wildcard prefixes" do
      compiled = compile(wildcard_fields())
      expected = Map.new(wildcard_fields(), &{&1.name, []})

      for document <- [%{}, %{"items" => []}, %{"items" => %{}}, %{"items" => nil}] do
        assert Mapper.map(document, compiled) == expected
      end
    end
  end

  describe "fused flat-map operations" do
    test "preserves top-level nil and permits excluded top-level keys to be elevated" do
      compiled =
        compile([
          Field.flat_map("preserved", path: "$", elevate_keys: ["metadata"]),
          Field.flat_map("excluded",
            path: "$",
            exclude_keys: ["level"],
            elevate_keys: ["metadata"]
          )
        ])

      document = %{
        "level" => nil,
        "metadata" => %{"level" => "nested", "region" => "us-east-1"}
      }

      assert Mapper.map(document, compiled) == %{
               "preserved" => %{"region" => "us-east-1"},
               "excluded" => %{"level" => "nested", "region" => "us-east-1"}
             }
    end

    test "top-level literal dotted keys win over nested elevated keys" do
      compiled = compile([Field.flat_map("attrs", path: "$", elevate_keys: ["metadata"])])

      document = %{
        "metadata" => %{"level" => "nested", "a" => %{"b" => 1}},
        "level" => "top",
        "a.b" => 2
      }

      assert Mapper.map(document, compiled) == %{
               "attrs" => %{"level" => "top", "a.b" => "2"}
             }
    end

    test "multiple elevate keys retain configured precedence and top-level collisions" do
      compiled =
        compile([
          Field.flat_map("attrs", path: "$", elevate_keys: ["first", "second"]),
          Field.json("json", path: "$", elevate_keys: ["first", "second"])
        ])

      document = %{
        "first" => %{"shared" => "first", "only_first" => 1},
        "second" => %{"shared" => "second", "only_second" => 2},
        "only_second" => "top"
      }

      assert Mapper.map(document, compiled) == %{
               "attrs" => %{
                 "shared" => "first",
                 "only_first" => "1",
                 "only_second" => "top"
               },
               "json" => %{
                 "shared" => "first",
                 "only_first" => 1,
                 "only_second" => "top"
               }
             }

      flat_document = %{
        "first.shared" => "first",
        "first.only_first" => 1,
        "second.shared" => "second",
        "second.only_second" => 2,
        "only_second" => "top"
      }

      assert Mapper.map(flat_document, compiled, flat_keys: true) == %{
               "attrs" => %{
                 "shared" => "first",
                 "only_first" => "1",
                 "only_second" => "top"
               },
               "json" => %{
                 "shared" => "first",
                 "only_first" => 1,
                 "only_second" => "top"
               }
             }
    end

    test "excluding an elevate key removes its subtree" do
      compiled =
        compile([
          Field.flat_map("attrs",
            path: "$",
            exclude_keys: ["metadata"],
            elevate_keys: ["metadata"]
          )
        ])

      assert Mapper.map(%{"metadata" => %{"level" => "info"}, "kept" => true}, compiled) == %{
               "attrs" => %{"kept" => "true"}
             }
    end
  end

  describe "precompiled flat-key paths" do
    test "one compiled resource keeps nested and literal flat lookups isolated" do
      compiled =
        compile([
          Field.string("first", path: "$.a.b", default: "missing"),
          Field.string("second", path: "$.a.b", default: "missing"),
          Field.string("coalesced", paths: ["$.a.missing", "$.a.b"], default: "missing")
        ])

      document = %{
        "a" => %{"b" => "nested", "missing" => "nested-first"},
        "a.b" => "flat",
        "a.missing" => "flat-first"
      }

      assert Mapper.map(document, compiled) == %{
               "first" => "nested",
               "second" => "nested",
               "coalesced" => "nested-first"
             }

      assert Mapper.map(document, compiled, flat_keys: true) == %{
               "first" => "flat",
               "second" => "flat",
               "coalesced" => "flat-first"
             }

      assert Mapper.map(%{"a" => %{"b" => "next"}, "a.b" => "next-flat"}, compiled) == %{
               "first" => "next",
               "second" => "next",
               "coalesced" => "next"
             }
    end
  end

  describe "allocation-free coercion paths" do
    test "ASCII, Unicode, and invalid UTF-8 transforms retain prior semantics" do
      compiled =
        compile([
          Field.string("ascii_unchanged", path: "$.ascii_unchanged", transform: "upcase"),
          Field.string("ascii_changed", path: "$.ascii_changed", transform: "downcase"),
          Field.string("unicode_up", path: "$.unicode_up", transform: "upcase"),
          Field.string("unicode_down", path: "$.unicode_down", transform: "downcase"),
          Field.string("invalid", path: "$.invalid", transform: "upcase")
        ])

      assert Mapper.map(
               %{
                 "ascii_unchanged" => "READY",
                 "ascii_changed" => "MixedCase123",
                 "unicode_up" => "Straße",
                 "unicode_down" => "İSTANBUL",
                 "invalid" => <<255>>
               },
               compiled
             ) == %{
               "ascii_unchanged" => "READY",
               "ascii_changed" => "mixedcase123",
               "unicode_up" => "STRASSE",
               "unicode_down" => "i̇stanbul",
               "invalid" => <<255>>
             }
    end

    test "streams compound values to the same JSON representation" do
      compiled = compile([Field.flat_map("attrs", path: "$.attributes")])

      document = %{
        "attributes" => %{
          "complex" => [%{"z" => 1, "a" => "quote\""}, nil, true, :ok, <<255>>]
        }
      }

      assert Mapper.map(document, compiled) == %{
               "attrs" => %{
                 "complex" => ~S([{"a":"quote\"","z":1},null,true,"ok",null])
               }
             }
    end

    test "borrowed numeric and boolean binaries handle valid, invalid, and boundary values" do
      compiled =
        compile([
          Field.uint64("uint_max", path: "$.uint_max"),
          Field.uint64("uint_negative", path: "$.uint_negative"),
          Field.uint64("uint_invalid_utf8", path: "$.uint_invalid_utf8"),
          Field.int32("int_min", path: "$.int_min"),
          Field.int32("int_overflow", path: "$.int_overflow"),
          Field.float64("float", path: "$.float"),
          Field.float64("float_invalid", path: "$.float_invalid"),
          Field.bool("bool_upper", path: "$.bool_upper"),
          Field.bool("bool_one", path: "$.bool_one"),
          Field.bool("bool_other", path: "$.bool_other"),
          Field.bool("bool_invalid_utf8", path: "$.bool_invalid_utf8")
        ])

      assert Mapper.map(
               %{
                 "uint_max" => Integer.to_string(@uint64_max),
                 "uint_negative" => "-1",
                 "uint_invalid_utf8" => <<255>>,
                 "int_min" => "-2147483648",
                 "int_overflow" => "2147483648",
                 "float" => "-12.5",
                 "float_invalid" => "twelve",
                 "bool_upper" => "TRUE",
                 "bool_one" => "1",
                 "bool_other" => "yes",
                 "bool_invalid_utf8" => <<255>>
               },
               compiled
             ) == %{
               "uint_max" => @uint64_max,
               "uint_negative" => 0,
               "uint_invalid_utf8" => 0,
               "int_min" => -2_147_483_648,
               "int_overflow" => 0,
               "float" => -12.5,
               "float_invalid" => 0.0,
               "bool_upper" => true,
               "bool_one" => true,
               "bool_other" => false,
               "bool_invalid_utf8" => false
             }
    end

    test "timestamp precision boundaries and saturating scale are stable" do
      compiled =
        compile([
          Field.datetime64("seconds_max", path: "$.seconds_max", precision: 9),
          Field.datetime64("millis_min", path: "$.millis_min", precision: 9),
          Field.datetime64("millis_max", path: "$.millis_max", precision: 9),
          Field.datetime64("micros_min", path: "$.micros_min", precision: 9),
          Field.datetime64("micros_max", path: "$.micros_max", precision: 9),
          Field.datetime64("nanos_min", path: "$.nanos_min", precision: 9),
          Field.datetime64("minimum_integer", path: "$.minimum_integer", precision: 9),
          Field.datetime64("space_iso8601", path: "$.space_iso8601", precision: 9)
        ])

      assert Mapper.map(
               %{
                 "seconds_max" => 9_999_999_999,
                 "millis_min" => 10_000_000_000,
                 "millis_max" => 9_999_999_999_999,
                 "micros_min" => 10_000_000_000_000,
                 "micros_max" => 9_999_999_999_999_999,
                 "nanos_min" => 10_000_000_000_000_000,
                 "minimum_integer" => -9_223_372_036_854_775_808,
                 "space_iso8601" => "2026-01-21 17:54:48.144506Z"
               },
               compiled
             ) == %{
               "seconds_max" => 9_223_372_036_854_775_807,
               "millis_min" => 10_000_000_000_000_000,
               "millis_max" => 9_223_372_036_854_775_807,
               "micros_min" => 10_000_000_000_000_000,
               "micros_max" => 9_223_372_036_854_775_807,
               "nanos_min" => 10_000_000_000_000_000,
               "minimum_integer" => -9_223_372_036_854_775_808,
               "space_iso8601" => 1_769_018_088_144_506_000
             }
    end
  end

  describe "reference-model parity" do
    property "optimized paths match a simple Elixir reference mapper" do
      compiled = compile(reference_fields())

      check all document <- reference_document_generator(), max_runs: 100 do
        assert Mapper.map(document, compiled) == reference_map(document)
      end
    end
  end

  defp compile(fields) do
    fields
    |> MappingConfig.new()
    |> Mapper.compile!()
  end

  defp cache_document(index) do
    case rem(index, 3) do
      0 ->
        %{
          "shared" => Map.new(@shared_keys, &{&1, "#{&1}-#{index}"}),
          "fallback" => "root-#{index}"
        }

      1 ->
        %{"fallback" => "root-#{index}"}

      2 ->
        %{
          "shared" => %{
            "a" => "a-#{index}",
            "b" => nil,
            "fallback" => "shared-#{index}"
          },
          "fallback" => "root-#{index}"
        }
    end
  end

  defp expected_cache_result(document) do
    shared = Map.get(document, "shared") || %{}

    Map.new(@shared_keys, fn key ->
      {key, Map.get(shared, key) || "missing"}
    end)
    |> Map.put(
      "fallback",
      Map.get(shared, "fallback") || Map.get(document, "fallback") || "none"
    )
  end

  defp wildcard_fields do
    [
      Field.array_string("names", path: "$.items[*].name"),
      Field.array_string("names_filtered", path: "$.items[*].name", filter_nil: true),
      Field.array_uint64("counts", path: "$.items[*].count"),
      Field.array_uint64("counts_filtered", path: "$.items[*].count", filter_nil: true),
      Field.array_float64("ratios", path: "$.items[*].ratio"),
      Field.array_float64("ratios_filtered", path: "$.items[*].ratio", filter_nil: true),
      Field.array_datetime64("times", path: "$.items[*].time"),
      Field.array_datetime64("times_filtered", path: "$.items[*].time", filter_nil: true),
      Field.array_json("raws", path: "$.items[*].raw"),
      Field.array_json("raws_filtered", path: "$.items[*].raw", filter_nil: true),
      Field.array_map("payloads", path: "$.items[*].payload"),
      Field.array_map("payloads_filtered", path: "$.items[*].payload", filter_nil: true),
      Field.array_flat_map("flat_attrs", path: "$.items[*].attrs"),
      Field.array_flat_map("flat_attrs_filtered",
        path: "$.items[*].attrs",
        filter_nil: true
      )
    ]
  end

  defp reference_fields do
    [
      Field.string("service",
        paths: ["$.resource.service.name", "$.service"],
        default: "unknown"
      ),
      Field.string("service_copy",
        paths: ["$.resource.service.name", "$.service"],
        default: "unknown"
      ),
      Field.uint64("count", path: "$.metrics.count", default: 7),
      Field.bool("sampled", path: "$.sampled", default: false),
      Field.array_string("event_names", path: "$.events[*].name", filter_nil: true),
      Field.flat_map("attributes", path: "$.attributes")
    ]
  end

  defp reference_document_generator do
    event_generator =
      one_of([
        map(
          one_of([string(:alphanumeric, max_length: 8), integer(-20..20), boolean()]),
          fn name ->
            %{"name" => name}
          end
        ),
        constant(%{}),
        constant("not-a-map")
      ])

    count_generator =
      one_of([
        integer(-100..10_000),
        map(integer(-100..10_000), &Integer.to_string/1),
        constant("invalid"),
        constant(nil)
      ])

    bool_generator =
      one_of([
        boolean(),
        integer(-1..1),
        member_of(["true", "TRUE", "1", "false", "yes"]),
        constant(nil)
      ])

    attribute_scalar = one_of([string(:alphanumeric, max_length: 8), integer(-20..20), boolean()])

    gen all service_mode <- member_of([:present, nil, :missing]),
            preferred <- string(:alphanumeric, max_length: 8),
            fallback <- string(:alphanumeric, min_length: 1, max_length: 8),
            count <- count_generator,
            sampled <- bool_generator,
            events <- list_of(event_generator, max_length: 6),
            attributes? <- boolean(),
            label <- attribute_scalar,
            nested_count <- integer(-20..20),
            values <- list_of(attribute_scalar, max_length: 4) do
      resource =
        case service_mode do
          :present -> %{"service" => %{"name" => preferred}}
          nil -> %{"service" => %{"name" => nil}}
          :missing -> %{}
        end

      %{
        "resource" => resource,
        "service" => fallback,
        "metrics" => %{"count" => count},
        "sampled" => sampled,
        "events" => events,
        "attributes" =>
          if(attributes?,
            do: %{
              "label" => label,
              "nested" => %{"count" => nested_count},
              "values" => values,
              "ignored" => nil
            },
            else: nil
          )
      }
    end
  end

  defp reference_map(document) do
    preferred = get_in(document, ["resource", "service", "name"])
    fallback = Map.get(document, "service")
    service = Enum.find([preferred, fallback], "unknown", &(&1 not in [nil, ""]))

    %{
      "service" => service,
      "service_copy" => service,
      "count" => reference_uint64(get_in(document, ["metrics", "count"]), 7),
      "sampled" => reference_bool(Map.get(document, "sampled"), false),
      "event_names" => reference_event_names(Map.get(document, "events")),
      "attributes" => reference_flat_map(Map.get(document, "attributes"))
    }
  end

  defp reference_uint64(nil, default), do: default

  defp reference_uint64(value, _default) when is_integer(value),
    do: value |> max(0) |> min(@uint64_max)

  defp reference_uint64(value, _default) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> min(integer, @uint64_max)
      _ -> 0
    end
  end

  defp reference_bool(nil, default), do: default
  defp reference_bool(value, _default) when is_boolean(value), do: value
  defp reference_bool(value, _default) when is_integer(value), do: value != 0

  defp reference_bool(value, _default) when is_binary(value) do
    String.downcase(value) in ["true", "1"]
  end

  defp reference_event_names(events) when is_list(events) do
    events
    |> Enum.map(fn
      event when is_map(event) -> Map.get(event, "name")
      _event -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&reference_string/1)
  end

  defp reference_event_names(_events), do: []

  defp reference_flat_map(value) when is_map(value) do
    flatten_reference_map(value, nil, %{})
  end

  defp reference_flat_map(_value), do: %{}

  defp flatten_reference_map(map, prefix, output) do
    Enum.reduce(map, output, fn {key, value}, output ->
      path = if prefix, do: "#{prefix}.#{key}", else: key

      cond do
        is_nil(value) ->
          output

        is_map(value) and map_size(value) > 0 ->
          flatten_reference_map(value, path, output)

        is_map(value) or is_list(value) ->
          Map.put(output, path, Jason.encode!(value))

        true ->
          Map.put(output, path, reference_string(value))
      end
    end)
  end

  defp reference_string(value) when is_binary(value), do: value
  defp reference_string(value) when is_integer(value), do: Integer.to_string(value)
  defp reference_string(value) when is_float(value), do: Float.to_string(value)
  defp reference_string(true), do: "true"
  defp reference_string(false), do: "false"
  defp reference_string(_value), do: ""
end
