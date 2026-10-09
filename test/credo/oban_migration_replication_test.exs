defmodule Logflare.CredoChecks.ObanMigrationReplicationTest do
  use Credo.Test.Case

  Code.require_file("lints/credo/module_aliases.ex")
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

  test "reports unwrapped Oban migrations called through an alias" do
    """
    defmodule Logflare.Repo.Migrations.AliasedOban do
      use Ecto.Migration

      alias Oban.Migration

      def up, do: Migration.up(version: 12)
    end

    defmodule Logflare.Repo.Migrations.RenamedOban do
      use Ecto.Migration

      alias Oban.Migration, as: ObanMigration

      def up, do: ObanMigration.up(version: 12)
    end

    defmodule Logflare.Repo.Migrations.MultiAliasedOban do
      use Ecto.Migration

      alias Oban.{Migration}

      def down, do: Migration.down(version: 1)
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issues(fn issues ->
      assert length(issues) == 3
      assert Enum.all?(issues, &(&1.message =~ "Oban.Migration."))
    end)
  end

  test "reports an Oban migration wrapped by a with_replicated_execute/1 from another module" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      def up do
        Other.with_replicated_execute(fn -> Oban.Migration.up(version: 12) end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issue(&assert(&1.message =~ "Oban.Migration.up"))
  end

  test "resolves aliases lexically at each call site" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      alias Oban.Migration

      def up, do: Migration.up(version: 12)

      defp unrelated do
        alias Other.Migration
        Migration.up(version: 1)
      end
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issue(&assert(&1.line_no == 6 and &1.message =~ "Oban.Migration.up"))
  end

  test "ignores aliases that are out of scope at the call site" do
    """
    defmodule Logflare.Repo.Migrations.NotOban do
      use Ecto.Migration

      def up do
        alias Oban.Migration
        :ok
      end

      def down do
        if true do
          alias Oban.Migration
        end

        Migration.down(version: 1)
      end

      def change do
        Migration.up(version: 12)
        alias Oban.Migration
      end
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> refute_issues()
  end

  test "reports an unwrapped Oban migration called through a require alias" do
    """
    defmodule Logflare.Repo.Migrations.AddObanJobsTable do
      use Ecto.Migration

      require Oban.Migration, as: ObanMigration

      def up, do: ObanMigration.up(version: 12)
    end
    """
    |> to_source_file()
    |> run_check(ObanMigrationReplication)
    |> assert_issue(&assert(&1.message =~ "Oban.Migration.up"))
  end
end
