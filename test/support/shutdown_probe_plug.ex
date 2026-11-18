defmodule Logflare.Test.ShutdownProbePlug do
  @behaviour Plug

  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolBuffer
  alias Logflare.Backends.Spool.Encoder
  alias Logflare.LogEvent
  alias LogflareWeb.Plugs.Shutdown

  @impl true
  def init(test_pid) when is_pid(test_pid), do: {test_pid, nil}
  def init(opts), do: opts

  @impl true
  def call(conn, {test_pid, spool_path}) do
    conn = Shutdown.call(conn, [])

    if conn.halted do
      conn
    else
      if conn.request_path == "/hold" do
        send(test_pid, {:held_http, self()})

        receive do
          :release -> append_event(spool_path)
        end
      end

      Plug.Conn.send_resp(conn, 200, "ok")
    end
  end

  defp append_event(nil), do: :ok

  defp append_event(path) do
    event = %LogEvent{
      body: %{"message" => Path.basename(path)},
      event_type: :log,
      ingested_at: DateTime.utc_now()
    }

    :ok =
      DurableBuffer.append_async(SpoolBuffer.name(), :probe, Encoder.encode_raw_chunk([event]))
  end
end
