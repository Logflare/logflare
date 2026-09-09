defmodule Logflare.Backends.Adaptor.S3Adaptor.HttpClient do
  @moduledoc false

  @behaviour ExAws.Request.HttpClient

  @finch_name Logflare.FinchS3
  @pool_timeout_marker "unable to provide a connection within the timeout"

  @impl ExAws.Request.HttpClient
  @spec request(atom(), binary(), binary(), [{binary(), binary()}], keyword()) ::
          {:ok, %{status_code: pos_integer(), headers: list(), body: binary()}}
          | {:error, %{reason: term()}}
  def request(method, url, body, headers, http_opts) do
    method
    |> Finch.build(url, headers, body)
    |> Finch.request(@finch_name, http_opts)
    |> normalize_response()
  rescue
    error in RuntimeError ->
      if pool_timeout?(error) do
        {:error, %{reason: :pool_timeout}}
      else
        reraise(error, __STACKTRACE__)
      end
  end

  defp pool_timeout?(%RuntimeError{message: message}) when is_binary(message),
    do: String.contains?(message, @pool_timeout_marker)

  defp pool_timeout?(_error), do: false

  defp normalize_response({:ok, %Finch.Response{} = response}) do
    {:ok,
     %{
       status_code: response.status,
       headers: response.headers,
       body: response.body
     }}
  end

  defp normalize_response({:error, reason}), do: {:error, %{reason: reason}}
end
