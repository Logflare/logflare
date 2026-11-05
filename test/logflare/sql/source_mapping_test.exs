defmodule Logflare.Sql.SourceMappingTest do
  use Logflare.DataCase

  alias Logflare.Sources.Catalog
  alias Logflare.Sql

  setup do
    user = insert(:user)
    insert(:source, user: user, name: "existing")
    Catalog.Cache.list_by_user(user.id)
    %{user: user}
  end

  test "retries missing mapped tokens against the primary", %{user: user} do
    source = insert(:source, user: user, name: "new_source")
    mapping = %{"old_name" => to_string(source.token)}

    assert {:ok, sql} = Sql.source_mapping("SELECT id FROM old_name", user.id, mapping)
    assert sql =~ "new_source"
    refute sql =~ "old_name"
  end

  test "returns a controlled error when a mapped source no longer exists", %{user: user} do
    mapping = %{"deleted_source" => Ecto.UUID.generate()}

    assert {:error, "can't find source deleted_source"} =
             Sql.source_mapping("SELECT id FROM deleted_source", user.id, mapping)
  end

  test "ignores missing mappings that are not referenced by the query", %{user: user} do
    mapping = %{"unused" => Ecto.UUID.generate()}
    assert {:ok, _sql} = Sql.source_mapping("SELECT id FROM existing", user.id, mapping)
  end
end
