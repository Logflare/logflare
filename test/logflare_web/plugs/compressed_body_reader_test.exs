defmodule LogflareWeb.Plugs.CompressedBodyReaderTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  @subject LogflareWeb.Plugs.CompressedBodyReader

  doctest @subject

  def conn(body, headers \\ []) do
    conn = Plug.Test.conn("POST", "/example", body)

    Enum.reduce(headers, conn, fn {key, value}, conn ->
      Plug.Conn.put_req_header(conn, key, value)
    end)
  end

  property "with no `content-encoding` header data is passed through as is" do
    check all(data <- binary()) do
      assert {:ok, read, _} = @subject.read_body(conn(data))
      assert read == data
    end
  end

  property "with `content-encoding: gzip` header data is passed through as is" do
    check all(data <- binary()) do
      compressed = :zlib.gzip(data)
      conn = conn(compressed, [{"content-encoding", "gzip"}])

      assert {:ok, read, _} = @subject.read_body(conn)
      assert read == data
    end
  end
end
