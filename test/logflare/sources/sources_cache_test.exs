defmodule Logflare.SourcesCacheTest do
  @moduledoc false
  import Logflare.Sources.Cache
  use Logflare.DataCase
  import Logflare.DummyFactory

  setup do
    r1 = build(:rule)
    r2 = build(:rule)
    u1 = insert(:user)
    source = insert(:source, token: Faker.UUID.v4(), rules: [r1, r2], user_id: u1.id)
    _s2 = insert(:source, token: Faker.UUID.v4(), user_id: u1.id)
    {:ok, source: source}
  end

  describe "source cache" do
    test "get_by_id/1", %{source: source} do
      left_source = get_by(token: source.token)
      assert left_source.id == source.id
      assert left_source.inserted_at == source.inserted_at
      assert is_list(left_source.rules)
      assert length(left_source.rules) == 2
    end
  end
end
