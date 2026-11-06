defmodule Logflare.Sql.SourceMappingTest do
  use Logflare.DataCase

  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.Sources
  alias Logflare.Sources.Catalog
  alias Logflare.Sql

  setup do
    user = insert(:user)
    insert(:source, user: user, name: "existing")
    Catalog.Cache.list_by_user(user.id)
    %{user: user}
  end

  for mapping <- [nil, %{}] do
    test "accepts #{inspect(mapping)} mappings for queries without saved source identities", %{
      user: user
    } do
      mapping = unquote(Macro.escape(mapping))
      assert {:ok, "SELECT 1"} = Sql.source_mapping("SELECT 1", user.id, mapping)

      assert {:ok, "SELECT id FROM existing"} =
               Sql.source_mapping("SELECT id FROM existing", user, mapping)
    end
  end

  test "catalog failures propagate through SQL consumers without list-processing crashes", %{
    user: user
  } do
    Cachex.clear!(Catalog.Cache)

    stub(Sources, :list_source_catalog_by_user, fn _ ->
      raise DBConnection.ConnectionError, message: "primary unavailable"
    end)

    for language <- [:bq_sql, :ch_sql, :pg_sql] do
      assert {:error, :database_unavailable} =
               Sql.transform(language, "SELECT id FROM existing", user)
    end

    assert {:error, :database_unavailable} = Sql.sources("SELECT id FROM existing", user)

    assert {:error, :database_unavailable} =
             Sql.source_mapping("SELECT id FROM existing", user, %{})

    assert {:error, :database_unavailable} =
             Catalog.Cache.source_name_by_token(user.id, Ecto.UUID.generate())

    changeset =
      %EndpointQuery{user: user, user_id: user.id}
      |> Ecto.Changeset.change(query: "SELECT id FROM existing")
      |> EndpointQuery.update_source_mapping()

    assert {"source catalog is temporarily unavailable", []} = changeset.errors[:query]
  end

  test "the primary retry also returns typed availability failures", %{user: user} do
    stub(Sources, :list_source_catalog_by_user, fn _ ->
      raise DBConnection.ConnectionError, message: "primary unavailable during retry"
    end)

    assert {:error, :database_unavailable} =
             Sql.source_mapping("SELECT id FROM old_name", user, %{
               "old_name" => Ecto.UUID.generate()
             })
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
