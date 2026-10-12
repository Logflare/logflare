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

  # Sentinel value substituted for credential-bearing headers by `redact/1`.
  @redacted_value "REDACTED"

  # Header names are case-insensitive. Keep this list intentionally explicit so
  # adding another credential-bearing header is a reviewed policy change.
  @sensitive_header_names MapSet.new(~w(
                            api-key
                            apikey
                            authorization
                            cookie
                            proxy-authorization
                            webhook-secret
                            x-access-token
                            x-amz-security-token
                            x-api-key
                            x-api-token
                            x-auth-token
                            x-hub-signature
                            x-hub-signature-256
                            x-secret-key
                            x-signature
                            x-webhook-secret
                          ))

  @doc """
  The sentinel substituted for a redacted header value.
  """
  @spec redacted_value() :: String.t()
  def redacted_value, do: @redacted_value

  @doc """
  What a form should display for a stored header value: blank stays blank so the field
  reads as empty, anything else becomes `redacted_value/0`.

  Unlike `redact/1` this ignores the header name — a stored value is never shown back
  to the user, whether or not the header is credential-bearing.
  """
  @spec mask_value(term()) :: term()
  def mask_value(value) when value in [nil, ""], do: value
  def mask_value(_value), do: @redacted_value

  @doc """
  Returns true when the header name carries credentials and must not be exposed.
  """
  @spec sensitive?(term()) :: boolean()
  def sensitive?(key), do: MapSet.member?(@sensitive_header_names, normalize_key(key))

  @doc """
  Replaces the values of credential-bearing headers with `redacted_value/0`.

  Used by adaptors' `redact_config/1` callbacks so secrets never reach an API
  response or the browser. Keys keep their original casing.
  """
  @spec redact(headers() | nil) :: map()
  def redact(nil), do: %{}

  def redact(headers) do
    for {key, value} <- headers, into: %{} do
      if sensitive?(key), do: {key, @redacted_value}, else: {key, value}
    end
  end

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
    Map.new(headers, fn {key, value} -> {normalize_key(key), value} end)
  end

  @doc """
  Canonicalizes a single header name to lower case, so a lookup matches a map
  built by `normalize_keys/1`.
  """
  @spec normalize_key(term()) :: String.t()
  def normalize_key(key), do: key |> to_string() |> String.downcase()
end
