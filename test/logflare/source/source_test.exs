defmodule Logflare.SourceTest do
  @moduledoc false
  alias Logflare.{Source, Sources}
  use Logflare.DataCase

  setup do
    u = insert(:user)
    s = insert(:source, token: "44a6851a-9a6f-49ee-822f-12c6f17bedee", rules: [], user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s]}
  end

  describe "Source" do
    test "to_bq_table_id/1", %{sources: [s | _]} do
      assert Source.to_bq_table_id(s) ==
               "`logflare-dev-238720`.#{s.user_id}_test.44a6851a_9a6f_49ee_822f_12c6f17bedee"
    end
  end
end
