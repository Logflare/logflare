defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.Buffer
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.LogEvent, as: LE
  import Logflare.DummyFactory

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)

    {:ok, sources: [s1]}
  end

  describe "Buffer" do
    test "GenServer", %{sources: [s1 | _]} do
      sid = s1.token
      rls = %RLS{source_id: sid}

      {:ok, _pid} = Buffer.start_link(rls)
      le = LE.make(%{"message" => "test"}, %{source: s1})
      le2 = LE.make(%{"message" => "test2"}, %{source: s1})
      Buffer.push(sid, le)
      Buffer.push(sid, le2)
      assert Buffer.get_count(sid) == 2
      assert le == Buffer.pop(sid)
      assert Buffer.get_count(sid) == 1
      assert le == Buffer.ack(sid, le.id)
      assert Buffer.get_count(sid) == 1
    end
  end
end
