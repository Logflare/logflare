defmodule LogflareWeb.NdjsonParserTest do
  @moduledoc false
  use LogflareWeb.ConnCase

  alias LogflareWeb.NdjsonParser
  alias Logflare.TestUtils

  test "decodes a non-gzipped ndjson log batch post request", %{conn: conn} do
    body = TestUtils.cloudflare_log_push_body(decoded: false) |> :zlib.gunzip()

    data = TestUtils.cloudflare_log_push_body(decoded: true)

    assert NdjsonParser.decode({:ok, body, conn}) == {:ok, data, conn}
  end
end
