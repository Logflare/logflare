defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.QueryTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.Query
  alias Logflare.Backends.Backend
  alias Logflare.Utils.SSRF

  @pool __MODULE__.Pool
  @url "https://8.8.8.8/select/42/prometheus"

  setup :verify_on_exit!

  test "sends raw PromQL to the explicit read endpoint with basic auth only" do
    query = ~S'sum(rate(requests_total{route="/a+b",env=~"prod|staging"}[5m]))'
    result = success("vector", [])

    expect(Tesla.Adapter.Finch, :call, fn env, options ->
      assert env.method == :post
      assert env.url == @url <> "/api/v1/query"

      assert URI.decode_query(env.body) == %{
               "query" => query,
               "time" => "2026-10-08T12:00:00Z",
               "timeout" => "10s"
             }

      assert Map.new(env.headers)["authorization"] ==
               "Basic " <> Base.encode64("user:secret")

      assert Map.new(env.headers)["content-type"] == "application/x-www-form-urlencoded"
      refute Map.has_key?(Map.new(env.headers), "x-write-only")
      refute Map.has_key?(Map.new(env.headers), "content-encoding")
      assert options[:receive_timeout] == 30_000
      assert options[:name] == Logflare.FinchDefaultHttp1
      reply(env, 200, result)
    end)

    backend =
      backend(%{
        username: "user",
        password: "secret",
        headers: %{"authorization" => "Bearer ignored", "x-write-only" => "ignored"}
      })

    assert {:ok, ^result} =
             Query.execute(backend, query, %{
               "time" => "2026-10-08T12:00:00Z",
               "timeout" => "10s",
               "backend_id" => 42,
               "query_url" => "https://other.example.com"
             })
  end

  test "range requests preserve proxy paths and numeric timestamps" do
    result = success("matrix", [%{"metric" => %{"job" => "api"}, "values" => [[10, "2"]]}])

    expect(Tesla.Adapter.Finch, :call, fn env, _options ->
      assert env.url == @url <> "/api/v1/query_range"

      assert URI.decode_query(env.body) == %{
               "query" => "up",
               "start" => "10",
               "end" => "20.5",
               "step" => "5s"
             }

      refute Map.has_key?(Map.new(env.headers), "authorization")
      reply(env, 200, result)
    end)

    assert {:ok, ^result} =
             Query.execute(backend(%{query_url: @url <> "/"}), "up", %{
               "start" => 10,
               "end" => 20.5,
               "step" => "5s"
             })
  end

  test "preserves result types, special values, warnings, and informational messages" do
    for {type, data} <- [
          {"vector", [%{"metric" => %{"env" => "prod"}, "value" => [123.5, "NaN"]}]},
          {"matrix", [%{"metric" => %{}, "values" => [[123.5, "+Inf"], [124.5, "-Inf"]]}]},
          {"scalar", [123.5, "42"]},
          {"string", [123.5, "hello"]}
        ] do
      result = Map.merge(success(type, data), %{"warnings" => ["warning"], "infos" => ["info"]})
      expect(Tesla.Adapter.Finch, :call, fn env, _options -> reply(env, 200, result) end)
      assert {:ok, ^result} = Query.execute(backend(), "up", %{})
    end
  end

  test "preserves native backend query errors and their HTTP status" do
    for status <- [400, 422, 503] do
      body = %{"status" => "error", "errorType" => "bad_data", "error" => "invalid expression"}
      expect(Tesla.Adapter.Finch, :call, fn env, _options -> reply(env, status, body) end)
      assert {:error, {^status, ^body}} = Query.execute(backend(), "invalid(", %{})
    end
  end

  test "normalizes non-JSON auth failures without returning response bodies" do
    for status <- [401, 403], headers <- [[], [{"content-type", "application/json"}]] do
      expect(Tesla.Adapter.Finch, :call, fn env, _ ->
        {:ok, %{env | status: status, body: "private upstream details", headers: headers}}
      end)

      assert {:error, {^status, %{"errorType" => "unauthorized"} = error}} =
               Query.execute(backend(), "up", %{})

      refute inspect(error) =~ "private upstream details"
    end
  end

  test "does not follow redirects or treat unexpected bodies as successful queries" do
    for {status, body} <- [{302, "redirect"}, {200, %{"unexpected" => "body"}}, {204, ""}] do
      expect(Tesla.Adapter.Finch, :call, fn env, _ ->
        {:ok,
         %{env | status: status, body: body, headers: [{"location", "https://other.example.com"}]}}
      end)

      assert {:error, {502, %{"errorType" => "bad_response"}}} =
               Query.execute(backend(), "up", %{})
    end
  end

  test "maps timeouts and transport failures to gateway errors without leaking credentials" do
    expect(Tesla.Adapter.Finch, :call, fn _env, _ -> {:error, :timeout} end)
    assert {:error, {504, %{"errorType" => "timeout"}}} = Query.execute(backend(), "up", %{})

    expect(Tesla.Adapter.Finch, :call, fn _env, _ -> {:error, {:connection_failed, "secret"}} end)
    assert {:error, {502, body}} = Query.execute(backend(), "up", %{})
    refute inspect(body) =~ "secret"
  end

  test "returns a native capacity error when the HTTP/1 connection pool is exhausted" do
    start_supervised!(
      {Finch, name: @pool, pools: %{default: [protocols: [:http1], size: 1, count: 1]}}
    )

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    parent = self()

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)
        send(parent, :pool_busy)

        receive do
          :release -> :gen_tcp.close(socket)
        end
      end)

    holder =
      Task.async(fn ->
        request = Mimic.call_original(Finch, :build, [:get, "http://127.0.0.1:#{port}/", [], nil])
        Mimic.call_original(Finch, :request, [request, @pool, [receive_timeout: 5_000]])
      end)

    try do
      assert_receive :pool_busy, 1_000

      stub(SSRF, :safe_resolve, fn "victoriametrics.test" -> {:ok, {127, 0, 0, 1}} end)

      stub(Finch, :build, fn method, url, headers, body ->
        Mimic.call_original(Finch, :build, [method, url, headers, body])
      end)

      stub(Finch, :request, fn request, pool, options ->
        Mimic.call_original(Finch, :request, [request, pool, options])
      end)

      expect(Tesla.Adapter.Finch, :call, fn env, options ->
        options = Keyword.merge(options, name: @pool, pool_timeout: 50)
        Mimic.call_original(Tesla.Adapter.Finch, :call, [env, options])
      end)

      assert {:error,
              {503,
               %{
                 "status" => "error",
                 "errorType" => "unavailable",
                 "error" => "VictoriaMetrics query capacity is temporarily unavailable"
               }}} =
               Query.execute(
                 backend(%{query_url: "http://victoriametrics.test:#{port}"}),
                 "up",
                 %{}
               )
    after
      send(server.pid, :release)
      Task.shutdown(holder, :brutal_kill)
      Task.shutdown(server, :brutal_kill)
      :gen_tcp.close(listener)
    end
  end

  test "does not normalize unrelated runtime errors" do
    expect(Tesla.Adapter.Finch, :call, fn _env, _options -> raise "unrelated boom" end)

    assert_raise RuntimeError, "unrelated boom", fn ->
      Query.execute(backend(), "up", %{})
    end
  end

  test "requires an explicitly configured safe read URL before sending requests" do
    reject(Tesla.Adapter.Finch, :call, 2)

    for url <- [
          nil,
          "",
          "ftp://8.8.8.8",
          "https://user:secret@8.8.8.8",
          "https://8.8.8.8?token=secret",
          "https://8.8.8.8/#fragment",
          "https://8.8.8.8:99999",
          "http://127.0.0.1:8428",
          "http://169.254.169.254"
        ] do
      assert {:error, {400, %{"status" => "error"}}} =
               Query.execute(backend(%{query_url: url}), "up", %{})
    end
  end

  test "rechecks the resolved destination before the transport call" do
    expect(SSRF, :safe_resolve, fn "8.8.8.8" -> {:ok, {8, 8, 8, 8}} end)
    expect(SSRF, :safe_resolve, fn "8.8.8.8" -> {:error, "private destination"} end)
    reject(Tesla.Adapter.Finch, :call, 2)
    assert {:error, {502, _body}} = Query.execute(backend(), "up", %{})
  end

  test "rejects malformed and ambiguous request parameters before sending requests" do
    reject(Tesla.Adapter.Finch, :call, 2)

    for query <- [nil, "", " \n ", ["up"], 1, <<255>>] do
      assert {:error, {400, _body}} = Query.execute(backend(), query, %{})
    end

    for params <- [
          nil,
          %{"time" => []},
          %{"time" => nil},
          %{"timeout" => ""},
          %{"start" => 1, "end" => 2},
          %{"step" => "5s"},
          %{"time" => 1, "start" => 1, "end" => 2, "step" => 1}
        ] do
      assert {:error, {400, _body}} = Query.execute(backend(), "up", params)
    end
  end

  @spec backend(map()) :: Backend.t()
  defp backend(config \\ %{}),
    do: %Backend{
      config: Map.merge(%{url: "https://1.1.1.1/api/v1/write", query_url: @url}, config)
    }

  @spec success(String.t(), term()) :: map()
  defp success(type, result),
    do: %{"status" => "success", "data" => %{"resultType" => type, "result" => result}}

  @spec reply(Tesla.Env.t(), pos_integer(), map()) :: Tesla.Env.result()
  defp reply(env, status, body),
    do:
      {:ok,
       %{
         env
         | status: status,
           body: Jason.encode!(body),
           headers: [{"content-type", "application/json"}]
       }}
end
