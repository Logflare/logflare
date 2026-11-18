defmodule Logflare.Test.ShutdownProbePlug do
  @behaviour Plug

  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolBuffer
  alias Logflare.Backends.Spool.Encoder
  alias Logflare.IngressGate
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
      case maybe_hold(conn, test_pid, spool_path) do
        :ok -> Plug.Conn.send_resp(conn, 200, "ok")
        {:error, :source_unavailable} -> Plug.Conn.send_resp(conn, 503, "Retry ingestion")
      end
    end
  end

  defp maybe_hold(%{request_path: "/hold"}, test_pid, spool_path) do
    send(test_pid, {:held_http, self()})

    receive do
      :release -> IngressGate.with_write(fn -> append_event(spool_path) end)
    end
  end

  defp maybe_hold(_conn, _test_pid, _spool_path), do: :ok

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
