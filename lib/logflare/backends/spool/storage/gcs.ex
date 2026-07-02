defmodule Logflare.Backends.Spool.Storage.GCS do
  @moduledoc false

  @behaviour Logflare.Backends.Spool.Storage

  alias GoogleApi.Storage.V1.Api.Objects

  @impl Logflare.Backends.Spool.Storage
  def put(bucket, key, body, opts) do
    headers = Keyword.get(opts, :headers, %{})
    content_type = Map.get(headers, "content-type", "application/octet-stream")

    with {:ok, conn} <- build_conn() do
      # Use "media" upload directly via Tesla — the generated _simple/_iodata helpers
      # both break for in-memory binary data (_simple treats body as a file path;
      # _iodata JSON-encodes everything via Poison). "media" upload sends the raw
      # binary as the request body with the correct Content-Type.
      #
      # Connection.new/1 returns a Tesla client with only the auth header; it does NOT
      # include the module-level BaseUrl plug from GoogleApi.Storage.V1.Connection.
      # We must build the absolute URL ourselves.
      base_url =
        Application.get_env(:google_api_storage, :base_url, "https://storage.googleapis.com/")

      encoded_bucket = URI.encode(bucket, &URI.char_unreserved?/1)
      url = "#{String.trim_trailing(base_url, "/")}/upload/storage/v1/b/#{encoded_bucket}/o"

      case Tesla.request(conn,
             method: :post,
             url: url,
             query: [uploadType: "media", name: key],
             headers: [{"Content-Type", content_type}],
             body: body
           ) do
        {:ok, %Tesla.Env{status: status}} when status in 200..299 -> {:ok, key}
        {:ok, %Tesla.Env{status: status, body: err}} -> {:error, {status, err}}
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
    token =
      case Application.get_env(:goth, :json) do
        nil ->
          # No credentials — assume local emulator which doesn't validate tokens
          "local-dev-token"

        _ ->
          case Goth.fetch(Logflare.Spool.Goth) do
            {:ok, %{token: t}} -> t
            {:error, reason} -> throw({:goth_fetch_error, reason})
          end
      end

    {:ok, GoogleApi.Storage.V1.Connection.new(token)}
  catch
    {:goth_fetch_error, reason} -> {:error, reason}
  end
end
