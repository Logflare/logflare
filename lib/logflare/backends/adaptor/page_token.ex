defmodule Logflare.Backends.Adaptor.PageToken do
  @moduledoc """
  Opaque, signed page tokens for paginated `c:Logflare.Backends.Adaptor.execute_query/3` results.

  A token is bound to the user it was issued to, so it cannot be replayed against another user's backend.
  """

  import Logflare.Utils.Guards

  alias Logflare.Backends.QueryError
  alias Phoenix.Token

  @salt "logflare query page token"
  @max_age 86_400

  @type offset_page :: {page_size :: pos_integer(), offset :: non_neg_integer()} | nil

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

  @doc """
  Resolves the `:page_size` and `:page_token` query opts into an offset page, for adaptors that
  page by wrapping the query in `LIMIT`/`OFFSET`. Returns `nil` when no `:page_size` is given.
  """
  @spec offset_page(pos_integer(), Keyword.t()) ::
          {:ok, offset_page()} | {:error, :invalid_page_token}
  def offset_page(user_id, opts) when is_list(opts) do
    case {Keyword.get(opts, :page_size), Keyword.get(opts, :page_token)} do
      {page_size, nil} when is_pos_integer(page_size) ->
        {:ok, {page_size, 0}}

      {page_size, page_token} when is_pos_integer(page_size) ->
        case decode(user_id, page_token) do
          {:ok, %{"offset" => offset}} when is_non_negative_integer(offset) ->
            {:ok, {page_size, offset}}

          _error ->
            {:error, :invalid_page_token}
        end

      _no_page ->
        {:ok, nil}
    end
  end

  @doc """
  Splits rows fetched with `LIMIT page_size + 1` into the page and the token for the next page.
  """
  @spec split_offset_page([term()], offset_page(), pos_integer()) :: {[term()], String.t() | nil}
  def split_offset_page(rows, nil, _user_id), do: {rows, nil}

  def split_offset_page(rows, {page_size, offset}, user_id) do
    case Enum.split(rows, page_size) do
      {page, []} -> {page, nil}
      {page, _more} -> {page, encode(user_id, %{"offset" => offset + page_size})}
    end
  end

  @spec invalid_token_error(module()) :: QueryError.t()
  def invalid_token_error(backend) do
    %QueryError{
      kind: :invalid_query,
      raw_error: :invalid_page_token,
      backend: backend,
      description: "Invalid or expired page token"
    }
  end
end
