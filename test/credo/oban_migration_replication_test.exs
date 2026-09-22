defmodule Logflare.CredoChecks.ObanMigrationReplicationTest do
  use Credo.Test.Case

  Code.require_file("lints/credo/replicated_execute_scope.ex")
  Code.require_file("lints/credo/oban_migration_replication.ex")

  alias Logflare.CredoChecks.ObanMigrationReplication

  setup_all do
    {:ok, _} = Application.ensure_all_started(:credo)
    :ok
  end

  test "accepts an Oban migration wrapped in with_replicated_execute/1" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      alias Logflare.Repo.Migrator

      def up do
        Migrator.with_replicated_execute(fn ->
          Oban.Migration.up(version: 12)
        end)
      end

      def down do
        Migrator.with_replicated_execute(fn ->
          Oban.Migration.down(version: 1)
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> refute_issues()
  end

  test "reports an unwrapped Oban migration" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      def up, do: Oban.Migration.up(version: 12)
      def down, do: Oban.Migration.down(version: 1)
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issues(fn issues ->
      assert length(issues) == 2
      assert Enum.any?(issues, &(&1.message =~ "Oban.Migration.up"))
      assert Enum.any?(issues, &(&1.message =~ "Oban.Migration.down"))
    end)
  end

  test "reports an Oban call that sits outside the wrapped block" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      alias Logflare.Repo.Migrator

      def up do
        Migrator.with_replicated_execute(fn ->
          execute("SELECT 1")
        end)

        Oban.Migration.up(version: 12)
      end
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issue(&assert(&1.message =~ "Oban.Migration.up"))
  end

  test "ignores unrelated module calls" do
    """
    defmodule Logflare.Repo.Migrations.Whatever do
      use Ecto.Migration

      def up, do: Something.Else.up(version: 12)
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> refute_issues()
  end
end
