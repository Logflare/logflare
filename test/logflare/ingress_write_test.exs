defmodule Logflare.IngressWriteTest do
  use Logflare.DataCase

  alias Logflare.Backends
  alias Logflare.IngressGate
  alias Logflare.SystemMetrics.AllLogsLogged

  test "the write seal rejects client ingestion but not internal routing" do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    event = build(:log_event, source: source, some: "event")

    IngressGate.quiesce()
    on_exit(fn -> :ets.insert(IngressGate, {:admission, :open}) end)
    assert {:ok, 1} = Backends.ingest_client_logs([event], source)

    IngressGate.seal()
    assert {:error, :source_unavailable} = Backends.ingest_client_logs([event], source)
    assert {:ok, 1} = Backends.ingest_logs([event], source)
  end
end
