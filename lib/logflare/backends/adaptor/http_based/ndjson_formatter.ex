defmodule Logflare.Backends.Adaptor.HttpBased.NdjsonFormatter do
  @moduledoc """
  Middleware encoding `Logflare.LogEvent`s as newline-delimited JSON, one event body per line.

  Sets the `content-type` header, so it must be used with `json: false` in
  `Logflare.Backends.Adaptor.HttpBased.Client.new/1` options.
  """

  require Logger

  alias Logflare.LogEvent

  @behaviour Tesla.Middleware

  @content_type "application/json"

  @impl Tesla.Middleware
  def call(env, next, _opts) do
    env
    |> put_content_type_header()
    |> Tesla.put_body(encode(env.body))
    |> Tesla.run(next)
  end

  @doc """
  Header names this formatter sets, so `HttpBased.Client` can drop any
  user-supplied copies and keep itself the single source (see
  `Logflare.Backends.Adaptor.HttpBased.Headers.drop_reserved/2`).
  """
  @spec reserved_headers() :: [String.t()]
  def reserved_headers, do: ["content-type"]

  # Strips any pre-existing content-type header case-insensitively before setting ours,
  # since Tesla.put_header/3 matches keys case-sensitively and would otherwise leave a
  # user-configured "Content-Type" header alongside this one.
  defp put_content_type_header(env) do
    headers = Enum.reject(env.headers, fn {k, _v} -> String.downcase(k) == "content-type" end)
    Tesla.put_header(%{env | headers: headers}, "content-type", @content_type)
  end

  @doc """
  Encodes `Logflare.LogEvent`s as newline-delimited JSON iodata, one event body
  per line. Any other term passes through unchanged.

  An event whose body cannot be JSON-encoded is dropped. The count of dropped
  events is logged per batch.
  """
  @spec encode([LogEvent.t()] | term()) :: iodata() | term()
  def encode([%LogEvent{} | _] = events) do
    {encoded, dropped_count} =
      Enum.reduce(events, {[], 0}, fn %LogEvent{body: body}, {acc, dropped_count} ->
        case Jason.encode_to_iodata(body) do
          {:ok, iodata} -> {[iodata | acc], dropped_count}
          {:error, _reason} -> {acc, dropped_count + 1}
        end
      end)

    if dropped_count > 0 do
      Logger.warning(
        "Dropped #{dropped_count} log events from an NDJSON batch: JSON encoding failed"
      )
    end

    encoded
    |> Enum.reverse()
    |> Enum.intersperse("\n")
  end

  def encode(term), do: term
end
