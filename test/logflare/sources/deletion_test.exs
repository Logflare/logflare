defmodule Logflare.Sources.DeletionTest do
  use Logflare.DataCase
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Sources
  alias Logflare.Sources.CleanupWorker
  alias Logflare.Sources.Source

  test "deletion queues a primary-database snapshot without external side effects" do
    user =
      insert(:user,
        bigquery_project_id: "original-project",
        bigquery_dataset_id: "original-dataset"
      )

    source = insert(:source, user: user)
    schema = insert(:source_schema, source: source)
    source_id = source.id

    stub(Logflare.Google.BigQuery, :delete_table, fn _, _, _ ->
      flunk("cleanup ran before commit")
    end)

    stale = %{source | user_id: -1, token: :stale}
    assert {:ok, %Source{id: ^source_id}} = Sources.delete_source(stale)
    refute Repo.get(Source, source.id)

    assert [job] = all_enqueued(worker: CleanupWorker)

    assert job.args == %{
             "source_id" => source.id,
             "source_token" => Atom.to_string(source.token),
             "source_schema_id" => schema.id,
             "bigquery" => %{
               "project_id" => "original-project",
               "dataset_id" => "original-dataset",
               "table_id" => GenUtils.format_table_name(source.token)
             }
           }
  end

  test "failed job insertion restores the source and its backend links" do
    user = insert(:user)
    backend = insert(:backend, user: user)
    source = insert(:source, user: user, backends: [backend])
    error = Ecto.Changeset.change(%Oban.Job{}) |> Ecto.Changeset.add_error(:args, "unavailable")

    expect(Oban, :insert, fn changeset ->
      assert Ecto.Changeset.get_field(changeset, :worker) == Oban.Worker.to_string(CleanupWorker)
      refute Repo.get(Source, source.id)
      refute Repo.exists?(from sb in "sources_backends", where: sb.source_id == ^source.id)
      {:error, error}
    end)

    assert {:error, ^error} = Sources.delete_source(source)

    assert %{backends: [%{id: backend_id}]} =
             Repo.get!(Source, source.id) |> Repo.preload(:backends)

    assert backend_id == backend.id
    refute_enqueued(worker: CleanupWorker)
  end

  test "an outer rollback also removes the cleanup job" do
    source = insert(:source, user: insert(:user))

    assert {:error, :cancelled} =
             Repo.transaction(fn ->
               assert {:ok, _} = Sources.delete_source(source)
               assert_enqueued(worker: CleanupWorker)
               Repo.rollback(:cancelled)
             end)

    assert Repo.get(Source, source.id)
    refute_enqueued(worker: CleanupWorker)
  end

  test "a database deletion failure never queues cleanup" do
    source = insert(:source, user: insert(:user))
    now = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    Repo.insert_all("source_backends", [
      %{source_id: source.id, inserted_at: now, updated_at: now}
    ])

    assert_raise Ecto.ConstraintError, ~r/source_backends_source_id_fkey/, fn ->
      Sources.delete_source(source)
    end

    assert Repo.get(Source, source.id)
    refute_enqueued(worker: CleanupWorker)
  end

  test "deleting an already deleted source does not enqueue another job" do
    source = insert(:source, user: insert(:user))
    assert {:ok, _} = Sources.delete_source(source)
    assert {:error, :not_found} = Sources.delete_source(source)
    assert [_job] = all_enqueued(worker: CleanupWorker)
  end

  test "Postgres deployments snapshot no BigQuery cleanup" do
    stub(Logflare.SingleTenant, :postgres_backend?, fn -> true end)
    source = insert(:source, user: insert(:user))
    assert {:ok, _} = Sources.delete_source(source)
    assert [job] = all_enqueued(worker: CleanupWorker)
    assert job.args["bigquery"] == nil
  end
end
