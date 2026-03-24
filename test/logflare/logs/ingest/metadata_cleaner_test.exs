defmodule Logflare.Logs.Ingest.MetadataCleanerTest do
  @moduledoc false
  use ExUnit.Case
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
  end
end
