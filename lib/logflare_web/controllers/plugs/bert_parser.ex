defmodule LogflareWeb.BertParser do
  @moduledoc """
  Parses BERT (http://bert-rpc.org) request body.

  Terms containing functions or structs are rejected with a 400.
  """

  @behaviour Plug.Parsers

  @struct_error_message "structs are not allowed in BERT payloads"

  def init(opts) do
    {body_reader, opts} = Keyword.pop(opts, :body_reader, {Plug.Conn, :read_body, []})
    {body_reader, opts}
  end

  def parse(conn, "application", "bert", _headers, {{mod, fun, args}, _opts}) do
    conn
    |> then(&apply(mod, fun, [&1 | args]))
    |> decode()
  end

  @doc false
  def parse(conn, _type, _subtype, _headers, _opts) do
    {:next, conn}
  end

  def decode({:ok, <<>>, conn}) do
    {:ok, %{}, conn}
  end

  def decode({:ok, <<131, 80, _uncompressed_size::32, _::binary>>, _conn}) do
    raise Plug.Parsers.ParseError,
      exception: %ArgumentError{message: "compressed BERT payloads are not supported"}
  end

  def decode({:ok, body, conn}) do
    term = Plug.Crypto.non_executable_binary_to_term(body, [:safe])
    if struct_term?(term), do: raise(ArgumentError, @struct_error_message)

    decoded = Bertex.Bert.decode(term)
    if struct_term?(decoded), do: raise(ArgumentError, @struct_error_message)

    {:ok, decoded, conn}
  rescue
    e ->
      reraise Plug.Parsers.ParseError, [exception: e], __STACKTRACE__
  end

  def decode({:more, _, conn}) do
    {:error, :too_large, conn}
  end

  def decode({:error, :timeout}) do
    raise Plug.TimeoutError
  end

  def decode({:error, _}) do
    raise Plug.BadRequestError
  end

  def atoms do
    # fixes a bug in Bertex where Bertex.safe_decode errors because
    # :bert and :dict atoms returned by :binary_to_term do not exist and are treated
    # as coming from the binary
    [:bert, :dict]
  end

  @spec struct_term?(term()) :: boolean()
  defp struct_term?(%{__struct__: _term}), do: true

  defp struct_term?(term) when is_map(term) do
    Enum.any?(term, fn {key, value} -> struct_term?(key) or struct_term?(value) end)
  end

  defp struct_term?([head | tail]), do: struct_term?(head) or struct_term?(tail)

  defp struct_term?(term) when is_tuple(term) do
    term |> Tuple.to_list() |> struct_term?()
  end

  defp struct_term?(_term), do: false
end
