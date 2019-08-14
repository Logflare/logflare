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
  end
end
