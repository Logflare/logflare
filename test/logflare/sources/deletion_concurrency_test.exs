defmodule Logflare.Sources.DeletionConcurrencyTest do
  use ExUnit.Case, async: false
  use Mimic

  import Ecto.Query
  import Logflare.Factory

  alias Ecto.Adapters.SQL.Sandbox
  alias Logflare.Backends.Backend
  alias Logflare.Repo
  alias Logflare.SingleTenant
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.TestUtils
  alias Logflare.User

  setup :set_mimic_global
  setup :verify_on_exit!

  test "backend attachment waits for deletion's initial source lock" do
    Sandbox.unboxed_run(Repo, fn ->
      user = insert(:user)
      backend = insert(:backend, user: user)
      source = insert(:source, user: user, backends: [backend])
      owner = self()

      on_exit(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.delete_all(
            from j in Oban.Job,
              where: fragment("?->>'source_id'", j.args) == ^to_string(source.id)
          )

          Repo.delete_all(from sb in "sources_backends", where: sb.source_id == ^source.id)
          Repo.delete_all(from s in Source, where: s.id == ^source.id)
          Repo.delete_all(from b in Backend, where: b.id == ^backend.id)
          Repo.delete_all(from u in User, where: u.id == ^user.id)
        end)
      end)

      expect(SingleTenant, :postgres_backend?, fn ->
        [[database_pid]] = Repo.query!("SELECT pg_backend_pid()").rows
        send(owner, {:source_locked, database_pid})

        receive do
          :resume_deletion -> false
        after
          10_000 -> flunk("deletion was not resumed")
        end
      end)

      deleting =
        Task.async(fn -> Sandbox.unboxed_run(Repo, fn -> Sources.delete_source(source) end) end)

      assert_receive {:source_locked, deleting_pid}, 5_000

      attaching =
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            [[database_pid]] = Repo.query!("SELECT pg_backend_pid()").rows
            send(owner, {:attaching, database_pid})

            try do
              Repo.insert_all("sources_backends", [
                %{source_id: source.id, backend_id: backend.id}
              ])
            rescue
              error in Postgrex.Error ->
                {:constraint, error.postgres.code, error.postgres.constraint}
            end
          end)
        end)

      try do
        assert_receive {:attaching, attaching_pid}, 5_000

        TestUtils.retry_assert(fn ->
          assert [[true]] =
                   Repo.query!("SELECT $1::int = ANY(pg_blocking_pids($2::int))", [
                     deleting_pid,
                     attaching_pid
                   ]).rows
        end)

        send(deleting.pid, :resume_deletion)
        assert {:ok, %Source{}} = Task.await(deleting)

        assert {:constraint, :foreign_key_violation, "sources_backends_source_id_fkey"} =
                 Task.await(attaching)

        refute Repo.get(Source, source.id)
      after
        Task.shutdown(deleting, :brutal_kill)
        Task.shutdown(attaching, :brutal_kill)
      end
    end)
  end
end
