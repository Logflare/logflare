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

  test "strips __LF_SOURCE from metadata and event_message, keeping it for routing", %{
    conn: conn
  } do
    token = Ecto.UUID.generate()
    body = Jason.encode!(%{"__LF_SOURCE" => token, "ClientIP" => "1.2.3.4"})

    assert {:ok, %{"batch" => [event]}, ^conn} = NdjsonParser.decode({:ok, body, conn})
    assert event["__LF_SOURCE"] == token
    assert event["metadata"] == %{"ClientIP" => "1.2.3.4"}
    refute event["event_message"] =~ "__LF_SOURCE"
    assert Jason.decode!(event["event_message"]) == %{"ClientIP" => "1.2.3.4"}
  end
end
