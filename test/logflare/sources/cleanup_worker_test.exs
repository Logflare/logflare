defmodule Logflare.Sources.CleanupWorkerTest do
  use Logflare.DataCase
  use Oban.Testing, repo: Logflare.Repo

  alias GoogleApi.BigQuery.V2.Api.Tables
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Sources
  alias Logflare.Sources.CleanupWorker
  alias Logflare.Sources.Counters

  setup do
    user =
      insert(:user,
        bigquery_project_id: "cleanup-project",
        bigquery_dataset_id: "cleanup-dataset"
      )

    source = insert(:source, user: user)
    assert {:ok, _} = Sources.delete_source(source)
    assert [job] = all_enqueued(worker: CleanupWorker)
    %{user: user, source: source, job: job}
  end

  test "deletes the captured table after both source and user are gone", %{
    user: user,
    source: source,
    job: job
  } do
    Repo.delete!(user)

    expect(Tables, :bigquery_tables_delete, fn _conn, project, dataset, table ->
      refute Repo.in_transaction?()
      assert project == "cleanup-project"
      assert dataset == "cleanup-dataset"
      assert table == GenUtils.format_table_name(source.token)
      {:ok, %{}}
    end)

    assert :ok = perform_job(CleanupWorker, job.args)
  end

  test "a missing table and repeated cleanup both succeed", %{job: job} do
    expect(Tables, :bigquery_tables_delete, 2, fn _, _, _, _ ->
      {:error, %Tesla.Env{status: 404}}
    end)

    assert :ok = perform_job(CleanupWorker, job.args)
    assert :ok = perform_job(CleanupWorker, job.args)
  end

  test "replayed cleanup cannot delete a replacement source's table", %{
    user: user,
    source: source,
    job: job
  } do
    insert(:plan, name: "Free")
    old_token = Atom.to_string(source.token)
    old_table = GenUtils.format_table_name(source.token)

    assert {:ok, replacement} =
             Sources.create_source(%{name: "replacement", token: old_token}, user)

    refute GenUtils.format_table_name(replacement.token) == old_table

    assert {:ok, updated} =
             Sources.update_source_by_user(replacement, %{name: "renamed", token: old_token})

    assert updated.token == replacement.token
    assert updated.name == "renamed"

    expect(Tables, :bigquery_tables_delete, 2, fn
      _conn, "cleanup-project", "cleanup-dataset", ^old_table ->
        {:ok, %{}}
    end)

    assert :ok = perform_job(CleanupWorker, job.args)
    assert :ok = perform_job(CleanupWorker, job.args)
  end

  test "Oban retries failed BigQuery deletion and eventually completes", %{job: job} do
    expect(Tables, :bigquery_tables_delete, fn _, _, _, _ -> {:error, %Tesla.Env{status: 503}} end)

    assert %{failure: 1} = Oban.drain_queue(queue: :default)
    assert %{state: "retryable", attempt: 1} = Repo.get!(Oban.Job, job.id)

    expect(Tables, :bigquery_tables_delete, fn _, _, _, _ -> {:ok, %{}} end)
    assert :ok = Oban.retry_job(job.id)
    assert %{success: 1} = Oban.drain_queue(queue: :default)
    assert %{state: "completed", attempt: 2} = Repo.get!(Oban.Job, job.id)
  end

  test "BigQuery failures do not persist request credentials in job errors", %{job: job} do
    expect(Tables, :bigquery_tables_delete, fn _, _, _, _ ->
      {:error, %Tesla.Env{status: 403, headers: [{"authorization", "secret"}], body: "secret"}}
    end)

    assert {:error, {:bigquery_delete_failed, 403}} = perform_job(CleanupWorker, job.args)
  end

  test "transport failures are retryable", %{job: job} do
    expect(Tables, :bigquery_tables_delete, fn _, _, _, _ -> {:error, :timeout} end)
    assert {:error, {:bigquery_delete_failed, :timeout}} = perform_job(CleanupWorker, job.args)
  end

  test "structured transport failures are recorded without raw response details", %{job: job} do
    expect(Tables, :bigquery_tables_delete, fn _, _, _, _ ->
      {:error, %Mint.HTTPError{reason: {:unexpected_response, "private response details"}}}
    end)

    assert %{failure: 1} = Oban.drain_queue(queue: :default)
    assert %{state: "retryable", errors: [%{"error" => error}]} = Repo.get!(Oban.Job, job.id)
    assert error =~ "{:bigquery_delete_failed, :transport_error}"
    refute error =~ "private response details"
  end

  test "incomplete cluster teardown retries before touching BigQuery", %{job: job} do
    expect(Logflare.Cluster.Utils, :erpc_multicall, fn _, CleanupWorker, :cleanup_local, _, _ ->
      [{:unreachable, {:error, :noconnection}}]
    end)

    stub(Tables, :bigquery_tables_delete, fn _, _, _, _ ->
      flunk("table deleted before teardown")
    end)

    assert {:error, {:cluster_cleanup_failed, [{:unreachable, {:error, :noconnection}}]}} =
             perform_job(CleanupWorker, job.args)
  end

  test "Postgres cleanup never calls BigQuery", %{job: job} do
    stub(Tables, :bigquery_tables_delete, fn _, _, _, _ ->
      flunk("unexpected BigQuery cleanup")
    end)

    assert :ok = perform_job(CleanupWorker, Map.put(job.args, "bigquery", nil))
  end

  test "local cleanup stops only the deleted source and invalidates cached configuration", %{
    source: source,
    job: job
  } do
    pid = start_source_tree(source.id)
    other_source = insert(:source, user_id: source.user_id)
    other_pid = start_source_tree(other_source.id)
    monitor = Process.monitor(pid)

    Counters.create(source.token)
    Counters.increment(source.token)
    {:ok, true} = Cachex.put(Sources.Cache, {:get_by, [[id: source.id]]}, {:cached, source})
    {:ok, true} = Cachex.put(Sources.Cache, {:get_by, [[token: source.token]]}, {:cached, source})

    {:ok, true} =
      Cachex.put(Sources.Cache, {:get_by_and_preload, [[id: source.id]]}, {:cached, source})

    assert :ok = CleanupWorker.cleanup_local(job.args)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :shutdown}
    assert Process.alive?(other_pid)
    assert Sources.Cache.get_by_id(source.id) == nil
    assert Sources.Cache.get_source_by_token(source.token) == nil
    assert Sources.Cache.get_by_and_preload(id: source.id) == nil
    assert Counters.get_inserts(source.token) == {:ok, 0}
    assert :ok = CleanupWorker.cleanup_local(job.args)
    assert {:error, :source_not_found} = Backends.start_for_ingest(source)
  end

  @spec start_source_tree(pos_integer()) :: pid()
  defp start_source_tree(id) do
    supervisor = {:via, PartitionSupervisor, {Backends.SourcesSup, id}}

    spec = %{
      id: {SourceSup, id},
      start: {Agent, :start_link, [fn -> nil end, [name: Backends.via_source(id, SourceSup)]]}
    }

    {:ok, pid} = DynamicSupervisor.start_child(supervisor, spec)
    on_exit(fn -> DynamicSupervisor.terminate_child(supervisor, pid) end)
    pid
  end
end
