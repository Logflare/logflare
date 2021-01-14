defmodule Logflare.SourcesCacheTest do
  @moduledoc false
  use Logflare.DataCase
  import Logflare.Factory

  setup do
    u1 = insert(:user)
    s01 = insert(:source, user_id: u1.id)
    s02 = insert(:source, user_id: u1.id)
    r1 = build(:rule, sink: s01.token)
    r2 = build(:rule, sink: s02.token)
    source = insert(:source, token: Faker.UUID.v4(), rules: [r1, r2], user_id: u1.id)
    _s2 = insert(:source, token: Faker.UUID.v4(), user_id: u1.id)
    {:ok, source: source}
  end

  describe "source cache" do
    test "get_by_id/1", %{source: source} do
      left_source =
        get_by(token: source.token)
        |> Repo.preload(:rules)

      assert left_source.id == source.id
      assert left_source.inserted_at == source.inserted_at
      assert is_list(left_source.rules)
      assert length(left_source.rules) == 2
    end
  end
end
