defmodule Logflare.Logs.Ingest.MetadataCleanerTest do
  @moduledoc false
  use ExUnit.Case
  use ExUnitProperties

  alias LogflareWeb.Logs.PayloadTestUtils
  alias Logflare.Logs.Ingest.MetadataCleaner, as: Cleaner

  describe "metadata payload cleaner" do
    test "removes nils from standard cloudlare metadata" do
      meta_with_nils =
        PayloadTestUtils.standard_metadata(:cloudflare)
        |> put_in(~w(request cf country), nil)
        |> put_in(~w(request cf colo), "")
        |> put_in(~w(request headers host), %{})
        |> put_in(~w(response headers vary), [])

      meta_cleaned = Cleaner.deep_reject_nil_and_empty(meta_with_nils)

      get_keys_in = &Map.keys(get_in(meta_cleaned, &1))

      assert Map.keys(meta_cleaned["request"]) == ~w[cf headers method url]
      assert "colo" not in get_keys_in.(~w(request cf))
      assert "country" not in get_keys_in.(~w(request cf))
      assert "host" not in get_keys_in.(~w(request headers))
      assert "vary" not in get_keys_in.(~w(response headers))
    end

    test "removes nils from standard elixir logger exception metadata" do
      meta_with_nils =
        PayloadTestUtils.standard_metadata(:elixir_logger_exception)
        |> update_in(
          ["stacktrace"],
          fn xs ->
            for map <- xs do
              %{map | "line" => nil, "module" => [], "arity_or_args" => %{}, "function" => ""}
            end
          end
        )
        |> update_in(["stacktrace"], &(&1 ++ [%{}, nil, [], "", {}]))
        |> put_in(["pid"], nil)

      meta_cleaned = Cleaner.deep_reject_nil_and_empty(meta_with_nils)

      assert length(meta_cleaned["stacktrace"]) == 2
      assert "pid" not in Map.keys(meta_cleaned)
      assert "line" not in Map.keys(hd(meta_cleaned["stacktrace"]))
    end

    test "removes nested empty containers" do
      meta_with_nils = %{"lists" => %{"nested" => [[]]}, "lists_not_empty" => ["1337"]}
      meta_cleaned = Cleaner.deep_reject_nil_and_empty(meta_with_nils)

      assert meta_cleaned == %{"lists_not_empty" => ["1337"]}

      meta_with_nils = %{
        "request" => %{
          "cf" => %{
            "asn" => 15_169,
            "clientTrustScore" => 1,
            "colo" => [""],
            "country" => nil
          }
        }
      }

      meta_cleaned = Cleaner.deep_reject_nil_and_empty(meta_with_nils)

      assert meta_cleaned == %{
               "request" => %{"cf" => %{"asn" => 15_169, "clientTrustScore" => 1}}
             }

      meta_with_nils =
        PayloadTestUtils.standard_metadata(:cloudflare)
        |> put_in(~w(request cf country), nil)
        |> put_in(~w(request cf colo), [""])
        |> put_in(~w(request headers host), %{"lists" => [[]]})
        |> put_in(~w(request headers host2), %{"lists" => %{"lists2" => [[]]}, "lists1.1" => [""]})
        |> put_in(~w(response headers vary), [["", [[]]]])
        |> put_in(~w(response headers vary2), [["", [[[["", [], [%{}]]]]]]])

      meta_cleaned = Cleaner.deep_reject_nil_and_empty(meta_with_nils)

      get_keys_in = &Map.keys(get_in(meta_cleaned, &1))

      assert Map.keys(meta_cleaned["request"]) == ~w[cf headers method url]
      assert "colo" not in get_keys_in.(~w(request cf))
      assert "country" not in get_keys_in.(~w(request cf))
      assert "host" not in get_keys_in.(~w(request headers))
      assert "vary" not in get_keys_in.(~w(response headers))
      assert "vary2" not in get_keys_in.(~w(response headers))
    end
  end

  describe "flatten/1" do
    test "flat map passes through unchanged" do
      assert Cleaner.flatten(%{"a" => 1, "b" => "hello"}) ==
               %{"a" => 1, "b" => "hello"}
    end

    test "nested maps produce dot-delimited keys" do
      assert Cleaner.flatten(%{"a" => %{"b" => %{"c" => 42}}}) ==
               %{"a.b.c" => 42}
    end

    test "lists use integer indices" do
      assert Cleaner.flatten(%{"tags" => ["alpha", "beta"]}) ==
               %{"tags.0" => "alpha", "tags.1" => "beta"}
    end

    test "nested maps inside lists" do
      input = %{"items" => [%{"name" => "a"}, %{"name" => "b"}]}

      assert Cleaner.flatten(input) ==
               %{"items.0.name" => "a", "items.1.name" => "b"}
    end

    test "empty map" do
      assert Cleaner.flatten(%{}) == %{}
    end

    test "mixed depth realistic body" do
      input = %{
        "event_message" => "hello",
        "timestamp" => 123,
        "metadata" => %{
          "level" => "info",
          "context" => %{"request_id" => "abc"}
        }
      }

      assert Cleaner.flatten(input) == %{
               "event_message" => "hello",
               "timestamp" => 123,
               "metadata.level" => "info",
               "metadata.context.request_id" => "abc"
             }
    end

    test "empty containers at leaves pass through as values" do
      assert Cleaner.flatten(%{"empty_map" => %{}, "empty_list" => []}) ==
               %{"empty_map" => %{}, "empty_list" => []}

      assert Cleaner.flatten(%{"a" => %{"b" => %{}, "c" => []}}) ==
               %{"a.b" => %{}, "a.c" => []}
    end

    test "nested lists use compound integer index paths" do
      assert Cleaner.flatten(%{"a" => [[1, 2], [3, 4]]}) ==
               %{"a.0.0" => 1, "a.0.1" => 2, "a.1.0" => 3, "a.1.1" => 4}
    end

    test "lists with mixed element types" do
      assert Cleaner.flatten(%{"xs" => [1, %{"k" => "v"}, [2, 3]]}) ==
               %{"xs.0" => 1, "xs.1.k" => "v", "xs.2.0" => 2, "xs.2.1" => 3}
    end

    test "literal dot-keys take precedence over nested-derived paths on collision" do
      assert Cleaner.flatten(%{"a.b" => :explicit, "a" => %{"b" => :nested}}) ==
               %{"a.b" => :explicit}
    end

    property "no output value is a non-empty map or non-empty list" do
      check all input <- nested_map_generator() do
        for {_k, v} <- Cleaner.flatten(input) do
          refute is_map(v) and not is_struct(v) and map_size(v) > 0
          refute match?([_ | _], v)
        end
      end
    end

    property "leaf count is preserved when no key collisions are possible" do
      check all input <- nested_map_generator(no_dot_keys: true) do
        assert map_size(Cleaner.flatten(input)) == count_leaves(input)
      end
    end

    property "every output key reconstructs to its value via the input path" do
      check all input <- nested_map_generator(no_dot_keys: true) do
        for {key, value} <- Cleaner.flatten(input) do
          assert reconstruct_path(input, String.split(key, ".")) == value
        end
      end
    end

    property "already-flat string-keyed maps pass through unchanged" do
      check all input <- flat_string_keyed_map_generator() do
        assert Cleaner.flatten(input) == input
      end
    end
  end

  defp scalar_value do
    one_of([
      integer(),
      string(:alphanumeric),
      boolean(),
      constant(nil),
      constant(%{}),
      constant([])
    ])
  end

  defp key_string(opts) do
    if Keyword.get(opts, :no_dot_keys, false) do
      string([?a..?z, ?0..?9, ?_], min_length: 1, max_length: 8)
    else
      string(:alphanumeric, min_length: 1, max_length: 8)
    end
  end

  defp nested_value_generator(opts) do
    tree(scalar_value(), fn child ->
      one_of([
        map_of(key_string(opts), child, max_length: 3),
        list_of(child, max_length: 3)
      ])
    end)
  end

  defp nested_map_generator(opts \\ []) do
    map_of(key_string(opts), nested_value_generator(opts),
      min_length: 1,
      max_length: 4
    )
  end

  defp flat_string_keyed_map_generator do
    map_of(string(:alphanumeric, min_length: 1, max_length: 8), scalar_value(), max_length: 6)
  end

  defp count_leaves(map) when is_map(map) and map_size(map) > 0 do
    Enum.reduce(map, 0, fn {_k, v}, acc -> acc + count_leaves(v) end)
  end

  defp count_leaves([_ | _] = list) do
    Enum.reduce(list, 0, fn v, acc -> acc + count_leaves(v) end)
  end

  defp count_leaves(_), do: 1

  defp reconstruct_path(value, []), do: value

  defp reconstruct_path(map, [segment | rest]) when is_map(map) do
    if Map.has_key?(map, segment) do
      reconstruct_path(Map.get(map, segment), rest)
    else
      :__not_found__
    end
  end

  defp reconstruct_path(list, [segment | rest]) when is_list(list) do
    case Integer.parse(segment) do
      {idx, ""} -> reconstruct_path(Enum.at(list, idx), rest)
      _ -> :__not_found__
    end
  end

  defp reconstruct_path(_, _), do: :__not_found__
end
