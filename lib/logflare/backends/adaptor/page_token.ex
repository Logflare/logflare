defmodule Logflare.Backends.Adaptor.PageToken do
  @moduledoc """
  Opaque, signed page tokens for paginated `c:Logflare.Backends.Adaptor.execute_query/3` results.

  A token is bound to the user it was issued to, so it cannot be replayed against another user's backend.
  """

  alias Phoenix.Token

  @salt "logflare query page token"
  @max_age 86_400

  @spec encode(pos_integer(), map()) :: String.t()
  def encode(user_id, payload) when is_integer(user_id) and is_map(payload) do
    Token.sign(LogflareWeb.Endpoint, @salt, {user_id, payload})
  end

  @spec decode(pos_integer(), String.t()) :: {:ok, map()} | {:error, :invalid_page_token}
  def decode(user_id, token) when is_integer(user_id) and is_binary(token) do
    case Token.verify(LogflareWeb.Endpoint, @salt, token, max_age: @max_age) do
      {:ok, {^user_id, payload}} when is_map(payload) -> {:ok, payload}
      _error -> {:error, :invalid_page_token}
    end
  end
end
