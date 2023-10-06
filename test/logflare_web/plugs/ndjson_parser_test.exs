defmodule Plugs.Parsers.NDJSONTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias Plug.Parsers.NDJSON
  alias Logflare.TestUtils

  describe "Plugs.Parsers.NDJSON" do
    test "decodes a non-gzipped ndjson log batch post request", %{conn: conn} do
      body = TestUtils.cloudflare_log_push_body(decoded: false) |> :zlib.gunzip()

      data = TestUtils.cloudflare_log_push_body(decoded: true)

      assert NDJSON.decode({:ok, body, conn}) == {:ok, data, conn}
    end
  end
end
