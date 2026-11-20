defmodule Logflare.TestUtils.Toxiproxy do
  @moduledoc """
  Controls a native Toxiproxy server through its HTTP API.

  Set `TOXIPROXY_URL` to override `http://127.0.0.1:8474`. Each proxy gets a
  unique name and an OS-assigned localhost port, and is deleted on test exit.
  Faults affect only that proxy; the shared server is never reset.

  HTTP requests use Finch's original implementation because the suite stubs
  Finch globally, including in ExUnit cleanup processes.
  """

  @type proxy :: %{
          name: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          api_url: String.t()
        }

  @spec create!(String.t()) :: proxy()
  def create!(upstream) do
    api_url = System.get_env("TOXIPROXY_URL", "http://127.0.0.1:8474")
    name = "logflare-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

    response =
      request!(api_url, :post, "/proxies", %{
        name: name,
        listen: "127.0.0.1:0",
        upstream: upstream
      })

    ExUnit.Callbacks.on_exit({__MODULE__, name}, fn ->
      request!(api_url, :delete, "/proxies/#{name}")
    end)

    [host, port] = String.split(response["listen"], ":")
    %{name: name, host: host, port: String.to_integer(port), api_url: api_url}
  end

  @spec enable!(proxy(), boolean()) :: :ok
  def enable!(proxy, enabled?) do
    request!(proxy.api_url, :post, "/proxies/#{proxy.name}", %{enabled: enabled?})
    :ok
  end

  @spec add_toxic!(proxy(), map()) :: map()
  def add_toxic!(proxy, toxic) do
    request!(proxy.api_url, :post, "/proxies/#{proxy.name}/toxics", toxic)
  end

  @spec remove_toxic!(proxy(), String.t()) :: :ok
  def remove_toxic!(proxy, name) do
    request!(proxy.api_url, :delete, "/proxies/#{proxy.name}/toxics/#{name}")
    :ok
  end

  @spec request!(String.t(), atom(), String.t(), map() | nil) :: term()
  defp request!(api_url, method, path, body \\ nil) do
    options = [base_url: api_url, url: path, method: method, retry: false, adapter: &request/1]
    options = if body, do: Keyword.put(options, :json, body), else: options
    response = Req.request!(options)

    if response.status in 200..299 do
      response.body
    else
      raise "Toxiproxy #{method} #{path} returned #{response.status}: #{inspect(response.body)}"
    end
  end

  @spec request(Req.Request.t()) :: {Req.Request.t(), Req.Response.t() | Exception.t()}
  defp request(request) do
    finch_request =
      Mimic.call_original(Finch, :build, [
        request.method,
        request.url,
        Req.get_headers_list(request),
        request.body
      ])

    case Mimic.call_original(Finch, :request, [
           finch_request,
           Req.Finch,
           [receive_timeout: 5_000, pool_timeout: 5_000]
         ]) do
      {:ok, response} ->
        {request,
         Req.Response.new(status: response.status, headers: response.headers, body: response.body)}

      {:error, exception} ->
        {request, exception}
    end
  end
end
