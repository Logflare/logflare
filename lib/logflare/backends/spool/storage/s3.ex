defmodule Logflare.Backends.Spool.Storage.S3 do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Storage

  @impl Logflare.Backends.Spool.Storage
  def put(bucket, key, body, opts) do
    headers = Keyword.get(opts, :headers, %{})

    case ExAws.S3.put_object(bucket, key, body, headers: headers) |> ExAws.request() do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  @impl Logflare.Backends.Spool.Storage
  def get(bucket, key) do
    case ExAws.S3.get_object(bucket, key) |> ExAws.request() do
      {:ok, %{body: raw}} -> {:ok, raw}
      {:error, reason} -> {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end
end
