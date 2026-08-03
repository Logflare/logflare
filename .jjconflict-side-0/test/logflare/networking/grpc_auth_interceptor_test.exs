defmodule Logflare.Networking.GrpcAuthInterceptorTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Networking.GrpcAuthInterceptor

  setup :verify_on_exit!

  describe "call/4" do
    test "injects authorization header into stream passed to next and returns its result" do
      stub(Goth, :fetch, fn _ -> {:ok, %Goth.Token{token: "test-token", type: "Bearer"}} end)

      stream = %GRPC.Client.Stream{headers: %{"x-existing" => "value"}}
      sentinel = %GRPC.Client.Stream{headers: %{"x-sentinel" => "yes"}}
      req = %{some: :request}
      test_pid = self()

      next = fn s, r ->
        send(test_pid, {:next_called, s, r})
        sentinel
      end

      result = GrpcAuthInterceptor.call(stream, req, next, [])

      assert result == sentinel
      assert_received {:next_called, called_stream, ^req}
      assert called_stream.headers["authorization"] == "Bearer test-token"
      assert called_stream.headers["x-existing"] == "value"
    end

    test "fetches token using phash2 partition" do
      test_pid = self()

      stub(Goth, :fetch, fn name ->
        send(test_pid, {:fetch_called, name})
        {:ok, %Goth.Token{token: "tok", type: "Bearer"}}
      end)

      GrpcAuthInterceptor.call(%GRPC.Client.Stream{}, nil, fn s, _ -> s end, [])

      expected = :erlang.phash2(self(), System.schedulers_online())
      assert_received {:fetch_called, {Logflare.Goth, ^expected}}
    end
  end
end
