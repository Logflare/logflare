defmodule Logflare.Test.ShutdownProbePlug do
  @behaviour Plug

  alias LogflareWeb.Plugs.Shutdown

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, test_pid) do
    conn = Shutdown.call(conn, [])

    if conn.halted do
      conn
    else
      if conn.request_path == "/hold" do
        send(test_pid, {:held_http, self()})

        receive do
          :release -> :ok
        end
      end

      Plug.Conn.send_resp(conn, 200, "ok")
    end
  end
end
