defmodule Logflare.Backends.Adaptor.HttpBased.Client do
  @moduledoc """
  A helper module for building HTTP Based Adaptors based on `Tesla`,
  designed for `Logflare.Backends.Adaptor.HttpBased.Pipeline`.
  """

  alias Logflare.Backends.Adaptor.HttpBased.EgressTracer
  alias Logflare.Backends.Adaptor.HttpBased.LogEventTransformer

  @type t() :: Tesla.Client.t()

  defguardp is_possible_pool(value)
            when not is_nil(value) and not is_boolean(value) and is_atom(value)

  @doc """
  Helper for building a `Tesla.Client` with appropriate middlewares & adapter.

  By default uses HTTP2 and JSON.
  Default formatter transforms LogEvents into their bodies when passed as request body,
  later handled by JSON encoder.
  """
  @spec new([opt]) :: t()
        when opt:
               {:url, binary()}
               | {:query, Tesla.Env.query()}
               | {:token, binary()}
               | {:gzip, true}
               | {:headers, Tesla.Env.headers()}
               | {:basic_auth, [username: binary(), password: binary()]}
               | {:formatter, module()}
               | {:pool_name, atom()}
               | {:http2, boolean()}
  def new(opts \\ []) do
    Tesla.client(
      [
        Tesla.Middleware.Telemetry,
        opts[:url] && {Tesla.Middleware.BaseUrl, opts[:url]},
        opts[:query] && {Tesla.Middleware.Query, opts[:query]},
        opts[:token] && {Tesla.Middleware.BearerAuth, token: opts[:token]},
        opts[:basic_auth] && {Tesla.Middleware.BasicAuth, opts[:basic_auth]},
        opts[:headers] && {Tesla.Middleware.Headers, opts[:headers]},
        Access.get(opts, :formatter, LogEventTransformer),
        Tesla.Middleware.JSON,
        opts[:gzip] && {Tesla.Middleware.CompressRequest, format: "gzip"},
        EgressTracer
      ]
      |> Enum.filter(& &1),
      adapter_config(Access.get(opts, :http2, true), opts[:pool_name])
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

  @doc """
  Sends log events via the client. The metadata is meant for `Logflare.Backends.Adaptor.HttpBased.EgressTracker`,
  should be passed as `opts: [metadata: metadata]` when making a request via Tesla.
  """
  @callback send_logs(config :: map(), log_events :: [Logflare.LogEvent.t()], metadata :: map()) ::
              :ok

  @doc """
  Tests the connectivity via adapter. Should make a request resembling one in `c:send/3` as close as possible.
  """
  @callback test_connection(config :: map()) :: :ok | {:error, term()}

  @optional_callbacks test_connection: 1
end
