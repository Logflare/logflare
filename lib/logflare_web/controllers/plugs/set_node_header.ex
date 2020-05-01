defmodule LogflareWeb.Plugs.SetNodeHeader do
  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    put_resp_header(conn, "logflare-node", Atom.to_string(Node.self()))
  end
end
