defmodule LogflareWeb.Plugs.CompressedBodyReader do
  @moduledoc """


  Gzip chunking is manually handled using inspiration from [sneako/plug_compressed_body_reader](https://github.com/sneako/plug_compressed_body_reader/blob/main/lib/plug_compressed_body_reader/gzip.ex)
  """

  def read_body(conn, opts \\ []) do
    content_encoding = Plug.Conn.get_req_header(conn, "content-encoding")

    opts = Keyword.merge([length: 8_000_000], opts)

    with {:ok, body, conn} <- Plug.Conn.read_body(conn, opts) do
      case try_decompress(body, content_encoding, opts) do
        {:ok, data} -> {:ok, data, conn}
        {:more, data} -> {:more, data, conn}
        {:error, _} = error -> error
      end
    end
  end

  defp try_decompress(body, [], _opts), do: {:ok, body}
  defp try_decompress(body, ["gzip"], opts), do: safe_gunzip(body, [type: :gzip] ++ opts)
  defp try_decompress(body, ["deflate"], opts), do: safe_gunzip(body, [type: :deflate] ++ opts)
  defp try_decompress(_body, [_], _opts), do: {:error, :not_supported}

  defp safe_gunzip(body, opts) do
    {:ok, z} = PlugCaisson.Zlib.init(opts)

    try do
      {return, data, _z} = PlugCaisson.Zlib.process(z, body, opts)
      {return, data}
    after
      PlugCaisson.Zlib.deinit(z)
    end
  end
end
