defmodule Logflare.Backends.QueryErrorTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.QueryError

  describe "JSON encoding" do
    test "encodes only the public message" do
      error = %QueryError{
        message: "raw backend message",
        code: :invalid_query,
        raw_error: %{"message" => "raw backend message"},
        backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
        description: "user-facing description"
      }

      assert Jason.encode!(error) == ~s({"message":"raw backend message"})
    end
  end
end
