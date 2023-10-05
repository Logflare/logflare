defmodule LogflareWeb.Plugs.CompressedBodyReader do
  def read_body(conn, opts \\ []) do
    content_encoding = Plug.Conn.get_req_header(conn, "content-encoding")

    with {:ok, body, conn} <- Plug.Conn.read_body(conn, opts) do
      case try_decompress(body, content_encoding) do
        {:ok, data} -> {:ok, data, conn}
        {:more, data} -> {:more, data, conn}
        {:error, _} = error -> error
      end
    end
  end

  defp try_decompress(data, []), do: {:ok, data}
  defp try_decompress(data, ["gzip"]), do: safe_gunzip(data)

  defp safe_gunzip(data) do
    z = :zlib.open()

    try do
      :zlib.inflateInit(z, 31)
      result = :zlib.safeInflate(z, data)
      :zlib.inflateEnd(z)

      result
    after
      :zlib.close(z)
    else
      {:finished, data} -> {:ok, IO.iodata_to_binary(data)}
      {:continue, data} -> {:more, IO.iodata_to_binary(data)}
      {:need_dictionary, _, _} -> {:error, :not_supported}
    end
  end
end
