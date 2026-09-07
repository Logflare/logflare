defmodule Logflare.Backends.Adaptor.HttpBased.HeadersTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.HttpBased.Headers

  describe "drop_reserved/2" do
    test "drops reserved names case-insensitively and keeps the rest" do
      headers = %{"Content-Type" => "text/plain", "X-Key" => "v"}

      assert Headers.drop_reserved(headers, ["content-type"]) == [{"X-Key", "v"}]
    end

    test "matches reserved names regardless of the reserved-list casing" do
      headers = %{"content-encoding" => "identity"}

      assert Headers.drop_reserved(headers, ["Content-Encoding"]) == []
    end

    test "preserves order and casing of a header list" do
      headers = [{"X-A", "1"}, {"Content-Type", "x"}, {"X-B", "2"}]

      assert Headers.drop_reserved(headers, ["content-type"]) == [{"X-A", "1"}, {"X-B", "2"}]
    end

    test "returns everything when nothing is reserved" do
      headers = [{"X-A", "1"}]

      assert Headers.drop_reserved(headers, []) == [{"X-A", "1"}]
    end
  end

  describe "normalize_keys/1" do
    test "downcases keys" do
      assert Headers.normalize_keys(%{"Content-Type" => "x", "X-Key" => "v"}) == %{
               "content-type" => "x",
               "x-key" => "v"
             }
    end

    test "collapses case-variant keys into one entry" do
      assert Headers.normalize_keys(%{"X-Foo" => "a", "x-foo" => "b"}) |> Map.keys() == ["x-foo"]
    end
  end
end
