defmodule Logflare.Backends.Adaptor.HttpBased.Client do
  @moduledoc """
  A helper module for building HTTP Based Adaptors based on `Tesla`,
  designed for `Logflare.Backends.Adaptor.HttpBased.Pipeline`.
  """

  alias Logflare.LogEvent
  alias Logflare.Backends.Adaptor.HttpBased.EgressTracer
  alias Logflare.Backends.Adaptor.HttpBased.LogEventTransformer
  alias Logflare.Backends.Backend

  @type t() :: Tesla.Client.t()

  @type opts :: [option()]

  @type option ::
          {:url, binary()}
          | {:query, Tesla.Env.query()}
          | {:token, binary()}
          | {:gzip, boolean()}
          | {:json, boolean()}
          | {:headers, %{String.t() => String.t()} | Tesla.Env.headers()}
          | {:basic_auth, [username: binary(), password: binary()]}
          | {:formatter, Tesla.Client.middleware()}
          | {:pool_name, atom()}
          | {:http2, boolean()}

  defguardp is_possible_pool(value)
            when not is_nil(value) and not is_boolean(value) and is_atom(value)

  @doc """
  Helper for building a `Tesla.Client` with appropriate middlewares & adapter.

  Default formatter transforms LogEvents into their bodies when passed as request body,
  later handled by JSON encoder.

  ## Options

  * `:url` - Sets the base URL for all requests.
  * `:query` - Sets query parameters to be added to all requests.
  * `:token` - Sets a bearer token for authentication.
  * `:gzip` - Enables gzip compression for request bodies.
  * `:json` - Enables JSON encoding for request bodies and decoding for response bodies. Defaults to `true`.
  * `:headers` - Sets headers to be added to all requests.
  * `:basic_auth` - Sets basic authentication credentials.
  * `:formatter` - A custom formatter for the request body. Defaults to `#{inspect(LogEventTransformer)}`.
  * `:pool_name` - An override for the name of the Finch pool to use for requests.
  * `:http2` - Whether to use HTTP/2. Defaults to `true`.
  """
  @spec new(opts()) :: t()
  def new(opts \\ []) do
    headers_middleware =
      case opts[:headers] do
        nil -> nil
        [] -> nil
        [_ | _] = headers -> {Tesla.Middleware.Headers, headers}
        %{} = map when map_size(map) == 0 -> nil
        %{} = headers -> {Tesla.Middleware.Headers, Enum.to_list(headers)}
      end

    Tesla.client(
      [
        Tesla.Middleware.Telemetry,
        opts[:url] && {Tesla.Middleware.BaseUrl, opts[:url]},
        opts[:query] && {Tesla.Middleware.Query, opts[:query]},
        opts[:token] && {Tesla.Middleware.BearerAuth, token: opts[:token]},
        opts[:basic_auth] && {Tesla.Middleware.BasicAuth, opts[:basic_auth]},
        headers_middleware,
        Keyword.get(opts, :formatter, LogEventTransformer),
        Keyword.get(opts, :json, true) && Tesla.Middleware.JSON,
        opts[:gzip] && {Tesla.Middleware.CompressRequest, format: "gzip"},
        EgressTracer
      ]
      |> Enum.filter(& &1),
      adapter_config(Keyword.get(opts, :http2, true), opts[:pool_name])
    )
  end

  defp adapter_config(http2?, pool_name) do
    cond do
      is_possible_pool(pool_name) ->
        {Tesla.Adapter.Finch, name: pool_name, receive_timeout: 5_000}

      http2? ->
        {Tesla.Adapter.Finch, name: Logflare.FinchDefault, receive_timeout: 5_000}

      true ->
        {Tesla.Adapter.Finch, name: Logflare.FinchDefaultHttp1, receive_timeout: 5_000}
    end
  end

  @spec send_events(module(), Backend.t(), [LogEvent.t()], map()) :: Tesla.Env.result()
  def send_events(module, backend, events, metadata \\ %{}) do
    metadata =
      (backend.metadata || %{})
      |> Map.new(fn {k, v} -> {"backend.#{k}", v} end)
      |> Map.merge(metadata)

    backend
    |> module.client_opts()
    |> new()
    |> Tesla.request(method: :post, url: "", body: events, opts: [metadata: metadata])
  end

  @doc """
  Callback providing options for `new/1`, allowing to create the client.
  """
  @callback client_opts(Backend.t()) :: opts()
end
