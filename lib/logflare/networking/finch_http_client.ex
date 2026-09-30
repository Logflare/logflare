defmodule Logflare.Networking.FinchHttpClient do
  @moduledoc """
  `use`-able `ExAws.Request.HttpClient` bound to a specific Finch pool, so
  each ExAws caller (S3 destination adaptor, spool S3/SQS, ...) can run its
  own isolated pool instead of ExAws's default Hackney client and its
  single shared connection pool.

      defmodule MyHttpClient do
        use Logflare.Networking.FinchHttpClient, finch_name: MyFinchPool
      end
  """

  @pool_timeout_marker "unable to provide a connection within the timeout"

  defmacro __using__(opts) do
    finch_name = Keyword.fetch!(opts, :finch_name)

    quote do
      @behaviour ExAws.Request.HttpClient

      @finch_name unquote(finch_name)

      @impl ExAws.Request.HttpClient
      @spec request(atom(), binary(), binary(), [{binary(), binary()}], keyword()) ::
              {:ok, %{status_code: pos_integer(), headers: list(), body: binary()}}
              | {:error, %{reason: term()}}
      def request(method, url, body, headers, http_opts) do
        method
        |> Finch.build(url, headers, body)
        |> Finch.request(@finch_name, http_opts)
        |> unquote(__MODULE__).normalize_response()
      rescue
        error in RuntimeError ->
          if unquote(__MODULE__).pool_timeout?(error) do
            {:error, %{reason: :pool_timeout}}
          else
            reraise(error, __STACKTRACE__)
          end
      end
    end
  end

  @doc false
  @spec pool_timeout?(Exception.t()) :: boolean()
  def pool_timeout?(%RuntimeError{message: message}) when is_binary(message),
    do: String.contains?(message, @pool_timeout_marker)

  def pool_timeout?(_error), do: false

  @doc false
  @spec normalize_response({:ok, Finch.Response.t()} | {:error, term()}) ::
          {:ok, %{status_code: pos_integer(), headers: list(), body: binary()}}
          | {:error, %{reason: term()}}
  def normalize_response({:ok, %Finch.Response{} = response}) do
    {:ok,
     %{
       status_code: response.status,
       headers: response.headers,
       body: response.body
     }}
  end

  def normalize_response({:error, reason}), do: {:error, %{reason: reason}}
end
