defmodule Logflare.Changefeeds.Setup do
  use Logflare.Commons

  def after_connect!(%DBConnection{} = db_conn) do
    set_client_session_node!(db_conn)
  end

  def set_client_session_node!(%DBConnection{} = db_conn) do
    Postgrex.query!(db_conn, "SET SESSION logflare.node_id = '#{Node.self()}'", [])
  end
end
