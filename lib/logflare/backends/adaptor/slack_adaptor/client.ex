defmodule Logflare.Backends.Adaptor.SlackAdaptor.Client do
  @moduledoc false

  use Tesla
  require Logger

  plug Tesla.Middleware.Retry,
    delay: 500,
    max_retries: 10,
    max_delay: 4_000,
    should_retry: fn
      {:ok, %{status: status}} when status in 400..599 -> true
      {:ok, _} -> false
      {:error, _} -> true
    end

  plug Tesla.Middleware.JSON

  def send(url, %{blocks: _} = body) when is_binary(url) do
    request(method: "post", url: url, body: body)
  end
end
