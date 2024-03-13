defmodule Logflare.Source.RecentLogsServerTest do
  @moduledoc false
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources
  use LogflareWeb.ConnCase
  test "push_many/2, list/1" do
    user = insert(:user)
    source = insert(:source, user: user)
    start_supervised!({RecentLogsServer, %{source: source}})

    assert [] = RecentLogsServer.list(source)
    le = build(:log_event, source: source)
    assert :ok = RecentLogsServer.push_many(source, [le])
    assert [_] = RecentLogsServer.list(source)
    assert [_] = RecentLogsServer.list_for_cluster(source.token)
  end
end
