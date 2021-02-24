defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  use LogflareWeb.ChannelCase
  use Logflare.Commons
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.BigQuery.Buffer
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory

  setup do
    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, s1} = Sources.create_source(params_for(:source), u1)
    s1 = Sources.preload_defaults(s1)
    rls = %RLS{source_id: s1.token}
    {:ok, _} = RLS.start_link(rls)
    # {:ok, _} = Sources.BuffersCache.start_link(rls)
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
