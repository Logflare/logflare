defmodule LogflareGrpc.IdentityCompressor do
  @behaviour GRPC.Compressor

  def name do
    "identity"
  end

  def compress(data) do
    data
  end

  def decompress(data) do
    data
  end
end
