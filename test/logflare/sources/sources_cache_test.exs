defmodule Logflare.SourcesCacheTest do
  @moduledoc false
  import Logflare.Sources.Cache
  use Logflare.DataCase

  setup do
    insert(:plan)
    u1 = insert(:user)
    s01 = insert(:source, user_id: u1.id)
    s02 = insert(:source, user_id: u1.id)
    r1 = build(:rule, sink: s01.token)
    r2 = build(:rule, sink: s02.token)
    source = insert(:source, token: TestUtils.gen_uuid(), rules: [r1, r2], user_id: u1.id)
    _s2 = insert(:source, token: TestUtils.gen_uuid(), user_id: u1.id)
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

    test "get_by_for_ingest/1 populates transform parsed virtuals" do
      user = insert(:user)

      source =
        insert(:source,
          user_id: user.id,
          token: TestUtils.gen_uuid(),
          transform_copy_fields: "service:m.routing.service",
          transform_drop_fields: "service\nm.routing.region",
          transform_key_values: "project:enriched"
        )

      assert %{
               transform_copy_fields_parsed: [
                 %{from_path: ["service"], to_path: ["metadata", "routing", "service"]}
               ],
               transform_drop_fields_parsed: [
                 ["service"],
                 ["metadata", "routing", "region"]
               ],
               transform_key_values_parsed: [_ | _]
             } = get_by_for_ingest(token: source.token)
    end
  end

  describe "get_by_token_or_primary/1" do
    test "reads the primary and fills the entry when the cache holds nil", %{source: source} do
      token = TestUtils.gen_uuid_atom()
      assert get_by(token: token) == nil

      %{id: id} = insert(:source, token: token, user_id: source.user_id)

      assert get_by(token: token) == nil
      assert %{id: ^id} = get_by_token_or_primary(token)
      assert %{id: ^id} = get_by(token: token)
    end

    test "returns nil for a token that has no source" do
      assert get_by_token_or_primary(TestUtils.gen_uuid_atom()) == nil
    end

    test "does not read the primary for a cached source", %{source: %{id: id, token: token}} do
      assert %{id: ^id} = get_by(token: token)
      reject(Logflare.Sources, :get_by, 1)

      assert %{id: ^id} = get_by_token_or_primary(token)
    end

    test "returns the error and does not read the primary when the database is unavailable" do
      stub(Logflare.ContextCache, :apply_fun, fn _, _, _ -> {:error, :database_unavailable} end)
      reject(Logflare.Sources, :get_by, 1)

      assert get_by_token_or_primary(TestUtils.gen_uuid_atom()) ==
               {:error, :database_unavailable}
    end
  end
end
