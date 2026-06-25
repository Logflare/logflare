defmodule Logflare.Backends.Spool.Storage.GCS do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Storage

  alias GoogleApi.Storage.V1.Api.Objects
  alias GoogleApi.Storage.V1.Model.Object

  @impl Logflare.Backends.Spool.Storage
  def put(bucket, key, body, opts) do
    headers = Keyword.get(opts, :headers, %{})
    content_type = Map.get(headers, "content-type", "application/octet-stream")

    with {:ok, conn} <- build_conn() do
      Objects.storage_objects_insert_simple(
        conn,
        bucket,
        "multipart",
        %Object{name: key, contentType: content_type},
        body,
        []
      )
      |> case do
        {:ok, object} -> {:ok, object}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl Logflare.Backends.Spool.Storage
  def get(bucket, key) do
    with {:ok, conn} <- build_conn() do
      case Objects.storage_objects_get(conn, bucket, key, alt: "media") do
        {:ok, %Tesla.Env{body: body}} -> {:ok, body}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp build_conn do
    with {:ok, %{token: token}} <- Goth.fetch(Logflare.Goth) do
      {:ok, GoogleApi.Storage.V1.Connection.new(token)}
    end
  end
end
