defmodule Logflare.Networking.GrpcAuthInterceptor do
  @moduledoc false

  @behaviour GRPC.Client.Interceptor

  @impl GRPC.Client.Interceptor
  def init(opts), do: opts

  @impl GRPC.Client.Interceptor
  def call(stream, req, next, _opts) do
    partition = :erlang.phash2(self(), System.schedulers_online())
    {:ok, token} = Goth.fetch({Logflare.Goth, partition})
    stream = GRPC.Client.Stream.put_headers(stream, %{"authorization" => "Bearer #{token.token}"})
    next.(stream, req)
  end
end
