defmodule LogflareWeb.Plugs.CompressedBodyReader do
  @moduledoc """


  Gzip chunking is manually handled using inspiration from [sneako/plug_compressed_body_reader](https://github.com/sneako/plug_compressed_body_reader/blob/main/lib/plug_compressed_body_reader/gzip.ex)
  """

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
  defp try_decompress(data, ["gzip"]), do: gunzip(data)
  defp try_decompress(data, ["deflate"]), do: inflate(data)

  @max_wbits 15
  @max_chunk_count 10

  defp gunzip(data), do: safe_gunzip(data, @max_wbits + 16)
  defp inflate(data), do: safe_gunzip(data, @max_wbits)

  defp safe_gunzip(data, window_bits) do
    z = :zlib.open()

    try do
      :zlib.inflateInit(z, window_bits)
      result = chunked_inflate(z, data)
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

  defp chunked_inflate(_res, _z, curr_chunk, _acc) when curr_chunk == @max_chunk_count do
    raise RuntimeError, "max chunks reached"
  end

  defp chunked_inflate({:finished, output}, _z, _curr_chunk, acc) do
    {:finished, Enum.reverse([output | acc])}
  end

  defp chunked_inflate({:continue, output}, z, curr_chunk, acc) do
    z
    |> :zlib.safeInflate([])
    |> chunked_inflate(z, curr_chunk + 1, [output | acc])
  end

  # initial
  defp chunked_inflate(z, data) when is_binary(data) do
    z
    |> :zlib.safeInflate(data)
    |> chunked_inflate(z, 0, [])
  end
end
