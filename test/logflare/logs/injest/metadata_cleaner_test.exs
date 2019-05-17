defmodule Logflare.Logs.Injest.MetadataCleanerTest do
  @moduledoc false
  use ExUnit.Case
  alias LogflareWeb.Logs.PayloadTestUtils
  alias Logflare.Logs.Cleaner

  describe "metadata payload cleaner" do
    test "removes nils from standard cloudlare metadata" do
      meta_with_nils =
        PayloadTestUtils.standard_metadata(:cloudflare)
        |> put_in(~w(request cf country), nil)
        |> put_in(~w(request headers host), nil)
        |> put_in(~w(response headers vary), nil)

      meta_cleaned = Logflare.Logs.Cleaner.reject_empty_kvs(meta_with_nils)

      get_keys_in = &Map.keys(get_in(meta_cleaned, &1))

      assert Map.keys(meta_cleaned["request"])  == ~w[cf headers method url]
      assert "country" not in get_keys_in.(~w(request cf))
      assert "host" not in get_keys_in.(~w(request headers))
      assert "vary" not in get_keys_in.(~w(response headers))
    end

  end
end
