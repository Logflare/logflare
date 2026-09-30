defmodule Logflare.Backends.Spool.Storage.S3 do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Storage

  alias Logflare.Backends.Spool.HttpClient

  @impl Logflare.Backends.Spool.Storage
  def put(bucket, key, body, opts) do
    headers = Keyword.get(opts, :headers, %{})

    case ExAws.S3.put_object(bucket, key, body, put_object_opts(headers))
         |> ExAws.request(http_client: HttpClient) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @impl Logflare.Backends.Spool.Storage
  def get(bucket, key) do
    case ExAws.S3.get_object(bucket, key) |> ExAws.request(http_client: HttpClient) do
      {:ok, %{body: raw}} -> {:ok, raw}
      {:error, {:http_error, 404, _}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  # ExAws.S3.put_object/4 only recognizes flat opts like :content_type and
  # :content_encoding (see ExAws.S3.Utils.put_object_headers/1) — nesting
  # them under a :headers key, as the storage-agnostic caller's `headers`
  # map does, silently drops them instead of erroring.
  defp put_object_opts(headers) do
    [content_type: Map.get(headers, "content-type", "application/octet-stream")] ++
      case Map.get(headers, "content-encoding") do
        nil -> []
        encoding -> [content_encoding: encoding]
      end
  end
end
