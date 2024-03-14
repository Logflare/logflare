defmodule Logflare.Source.RecentLogsServerTest do
  @moduledoc false
  alias Logflare.Source.RecentLogsServer
  use LogflareWeb.ConnCase

  test "push/2, list/1" do
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({RecentLogsServer, source: source})

    assert [] = RecentLogsServer.list(source)
    le = build(:log_event, source: source)
    assert :ok = RecentLogsServer.push(source, [le])
    assert [_] = RecentLogsServer.list(source)
    assert [_] = RecentLogsServer.list_for_cluster(source)
    assert [_] = RecentLogsServer.list_for_cluster(source.token)
  end
end
