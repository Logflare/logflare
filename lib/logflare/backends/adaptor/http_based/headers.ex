defmodule Logflare.Backends.Adaptor.HttpBased.Headers do
  @moduledoc """
  Header normalization shared by the Tesla-based HTTP clients
  (`Logflare.Backends.Adaptor.WebhookAdaptor.Client` and
  `Logflare.Backends.Adaptor.HttpBased.Client`).

  Tesla middleware (e.g. `Tesla.Middleware.JSON`, `Tesla.Middleware.CompressRequest`,
  `Tesla.Middleware.BearerAuth`) set request headers by appending, and
  `Tesla.put_headers/2` appends rather than replaces. A user-supplied header of the
  same name therefore survives alongside the middleware's, producing a duplicate that
  some receivers concatenate into an unparseable value (e.g.
  "application/jsonapplication/json", yielding an empty parsed body). These helpers
  ensure such transport-owned headers have a single source.
  """

  @type header_list :: [{String.t(), term()}]
  @type headers :: %{optional(String.t()) => term()} | header_list()

  @doc """
  Drops the client-owned header names from user-supplied headers.

  `reserved` is the set of header names the active middleware will set for the
  request; dropping them (case-insensitively) leaves the client's value as the only
  source, so "the server wins" holds by construction rather than by header ordering.
  Remaining headers keep their original casing and order.
  """
  @spec drop_reserved(headers(), [String.t()]) :: header_list()
  def drop_reserved(headers, reserved) do
    reserved_set = MapSet.new(reserved, &String.downcase/1)

    for {key, value} <- headers,
        not MapSet.member?(reserved_set, String.downcase(to_string(key))),
        do: {key, value}
  end

  @doc """
  Canonicalizes user-supplied header names to lower case.

  HTTP header names are case-insensitive, so storing both `Content-Type` and
  `content-type` represents the same header twice. Downcasing keys collapses such
  case-variants into a single canonical entry and matches the form used on the wire.
  """
  @spec normalize_keys(map()) :: map()
  def normalize_keys(headers) when is_map(headers) do
    Map.new(headers, fn {key, value} -> {String.downcase(to_string(key)), value} end)
  end
end
