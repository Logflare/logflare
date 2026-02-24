defmodule LogflareWeb.JsonParser do
  @moduledoc """
  Implementation is taken from https://github.com/elixir-plug/plug/blob/v1.15.3/lib/plug/parsers/json.ex#L1

  Only changes are related to the error handling for BadRequestError.
  """
  require Logger
  @behaviour Plug.Parsers

  @impl true
  defdelegate init(opts), to: Plug.Parsers.JSON

  @impl true
  def parse(conn, "application", subtype, _headers, {{mod, fun, args}, decoder, opts}) do
    if subtype == "json" or String.ends_with?(subtype, "+json") do
      apply(mod, fun, [conn, opts | args]) |> decode(decoder, opts)
    else
      {:next, conn}
    end
  end

  def parse(conn, _type, _subtype, _headers, _opts) do
    {:next, conn}
  end

  defp decode({:ok, "", conn}, _decoder, _opts) do
    {:ok, %{}, conn}
  end

  defp decode({:ok, body, conn}, {module, fun, args}, opts) do
    nest_all = Keyword.get(opts, :nest_all_json, false)

    try do
      apply(module, fun, [body | args])
    rescue
      e -> reraise Plug.Parsers.ParseError, [exception: e], __STACKTRACE__
    else
      terms when is_map(terms) and not nest_all ->
        {:ok, terms, conn}

      terms ->
        {:ok, %{"_json" => terms}, conn}
    end
  end

  defp decode({:more, _, conn}, _decoder, _opts) do
    {:error, :too_large, conn}
  end

  defp decode({:error, :timeout}, _decoder, _opts) do
    raise Plug.TimeoutError
  end

  # add better error message for debugging body reader errors
  defp decode({:error, err}, _decoder, _opts) do
    raise __MODULE__.BadRequestError,
      message: "Body reader error: #{inspect(err)}",
      plug_status: 400
  end

  defmodule BadRequestError do
    @moduledoc false
    defexception [:message, :plug_status]
  end
end
