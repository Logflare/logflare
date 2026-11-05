defmodule Logflare.Sql.UserLookupTest do
  use Logflare.DataCase

  alias Logflare.Sources
  alias Logflare.Sql
  alias Logflare.Users

  setup do
    insert(:plan)
    user = insert(:user, bigquery_project_id: "original-project", bigquery_dataset_id: "logs")
    insert(:source, user: user, name: "events")
    Cachex.put!(Users.Cache, {:get, [user.id]}, {:cached, user})
    Sources.list_sources_by_user(user)

    %{user: user}
  end

  test "integer-user transformations preserve transaction read-your-writes", %{user: user} do
    assert {:ok, :ok} =
             Repo.transaction(fn ->
               user
               |> Ecto.Changeset.change(bigquery_project_id: "updated-project")
               |> Repo.update!()

               assert {:ok, sql} = Sql.transform(:bq_sql, "SELECT id FROM events", user.id)
               assert sql =~ "updated-project.logs."
               refute sql =~ "original-project"
               :ok
             end)

    assert {:cached, %{bigquery_project_id: "original-project"}} =
             Cachex.get!(Users.Cache, {:get, [user.id]})
  end

  test "integer-user transformations still reuse the cache outside transactions", %{user: user} do
    user
    |> Ecto.Changeset.change(bigquery_project_id: "updated-project")
    |> Repo.update!()

    refute Repo.in_transaction?()
    assert {:ok, sql} = Sql.transform(:bq_sql, "SELECT id FROM events", user.id)
    assert sql =~ "original-project.logs."
  end
end
