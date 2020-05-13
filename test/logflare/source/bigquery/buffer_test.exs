defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  use LogflareWeb.ChannelCase
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.BigQuery.Buffer
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    rls = %RLS{source_id: s1.token}

    {:ok, sources: [s1], args: rls}
  end

  describe "GenServer" do
    test "start_link, push, pop, ack, broadcast", %{sources: [s1 | _], args: rls} do
      sid = s1.token
      {:ok, _pid} = Buffer.start_link(rls)
      le = LE.make(%{"message" => "test"}, %{source: s1})
      le2 = LE.make(%{"message" => "test2"}, %{source: s1})
      Buffer.push(le)
      Buffer.push(le2)
      assert Buffer.get_count(sid) == 2
      assert le == Buffer.pop(sid)
      assert Buffer.get_count(sid) == 1
      assert {:ok, le} == Buffer.ack(sid, le.id)
      assert Buffer.get_count(sid) == 1
    end
  end
end
