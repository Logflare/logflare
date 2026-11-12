defmodule LogflareWeb.Plugs.Shutdown do
  @moduledoc """
  Wraps endpoint dispatch, including socket upgrades, before application work can begin.
  """
  @behaviour Plug

  alias Logflare.IngressGate
  alias Plug.Conn

  defmacro __before_compile__(_env) do
    quote do
      defoverridable call: 2

      def call(conn, opts) do
        case unquote(__MODULE__).call(conn, []) do
          %Conn{halted: true} = conn -> conn
          conn -> super(conn, opts)
        end
      end
    end
  end

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{path_info: [path]} = conn, _opts) when path in ["health", "ready"], do: conn

  def call(conn, _opts) do
    if IngressGate.http_admitted?() do
      conn
    else
      conn
      |> close_http1()
      |> Plug.Conn.put_resp_header("retry-after", "1")
      |> Plug.Conn.send_resp(503, "Server shutting down")
      |> Plug.Conn.halt()
    end
  end

  defp close_http1(conn) do
    case Plug.Conn.get_http_protocol(conn) do
      :"HTTP/1.1" -> Plug.Conn.put_resp_header(conn, "connection", "close")
      _ -> conn
    end
  end
end
