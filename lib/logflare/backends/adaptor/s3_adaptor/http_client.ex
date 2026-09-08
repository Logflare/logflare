defmodule Logflare.Backends.Adaptor.S3Adaptor.HttpClient do
  @moduledoc false

  @behaviour ExAws.Request.HttpClient

  @finch_name Logflare.FinchS3

  @impl ExAws.Request.HttpClient
  @spec request(atom(), binary(), binary(), [{binary(), binary()}], keyword()) ::
          {:ok, %{status_code: pos_integer(), headers: list(), body: binary()}}
          | {:error, %{reason: term()}}
  def request(method, url, body, headers, http_opts) do
    method
    |> Finch.build(url, headers, body)
    |> Finch.request(@finch_name, http_opts)
    |> normalize_response()
  end

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
