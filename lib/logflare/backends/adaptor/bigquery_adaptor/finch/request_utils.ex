defmodule Grpc.Client.Adapters.Finch.RequestUtils do
  def check_compression(headers, stream) do
    headers_map = Map.new(headers)
    encoding = headers_map["grpc-encoding"]

    if encoding do
      encoding = Enum.find(stream.accepted_compressors, nil, fn c -> c.name() == encoding end)
      Map.put(stream, :compressor, encoding)
    else
      stream
    end
  end

  def parse_headers(headers) do
    headers = GRPC.Transport.HTTP2.decode_headers(headers)

    if headers["grpc-status"] do
      grpc_status = String.to_integer(headers["grpc-status"])

      if grpc_status == GRPC.Status.ok() do
        {:ok, headers}
      else
        {:error, %GRPC.RPCError{status: grpc_status, message: headers["grpc-message"]}}
      end
    else
      {:ok, headers}
    end
  end
end
