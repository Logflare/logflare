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
    check all(data <- gen_payloads()) do
      assert {:ok, read, _} = @subject.read_body(conn(data))
      assert read == data
    end
  end

  property "with `content-encoding: gzip` header data is passed through as is" do
    check all(data <- gen_payloads()) do
      compressed = :zlib.gzip(data)
      conn = conn(compressed, [{"content-encoding", "gzip"}])

      assert {:ok, read, _} = @subject.read_body(conn)
      assert read == data
    end

    check all(data <- gen_max_chunk_payloads()) do
      compressed = :zlib.gzip(data)
      conn = conn(compressed, [{"content-encoding", "gzip"}])

      assert_raise RuntimeError, "max chunks reached", fn ->
        @subject.read_body(conn)
      end
    end
  end

  defp gen_payloads do
    gen all(res <- scale(binary(), &(&1 * 500))) do
      res
    end
  end

  defp gen_max_chunk_payloads do
    gen all(res <- binary(max_length: 110_000, min_length: 100_000)) do
      res
    end
  end
end
