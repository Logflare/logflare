defmodule LogflareWeb.ProtobufParser do
  @moduledoc """
  Parses protobuf request body

  Requires the protobuf schema to be assigned to the connection.
  """

  @behaviour Plug.Parsers

  def init(opts) do
    {body_reader, opts} = Keyword.pop(opts, :body_reader, {Plug.Conn, :read_body, []})
    {body_reader, opts}
  end

  def parse(conn, "application", "x-protobuf", _headers, {{mod, fun, args}, _opts}) do
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

  def decode({:ok, body, conn}) do
    protobuf_schema = Map.fetch!(conn.assigns, :protobuf_schema)
    {:ok, protobuf_schema.decode(body), conn}
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
end
