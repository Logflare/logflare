defmodule Logflare.CredoChecks.ReplicatedDdlExecuteTest do
  use Credo.Test.Case

  Code.require_file("lints/credo/module_aliases.ex")
  Code.require_file("lints/credo/replicated_execute_scope.ex")
  Code.require_file("lints/credo/replicated_ddl_execute.ex")

  alias Logflare.CredoChecks.ReplicatedDdlExecute

  setup_all do
    {:ok, _} = Application.ensure_all_started(:credo)
    :ok
  end

  test "accepts DDL strings wrapped in with_replicated_execute/1" do
    """
    defmodule Logflare.Repo.Migrations.DropAConstraint do
      use Ecto.Migration

      alias Logflare.Repo.Migrator

      def up do
        Migrator.with_replicated_execute(fn ->
          execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")

          alter table(:sources) do
            modify(:user_id, references(:users, on_delete: :delete_all))
          end
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> refute_issues()
  end

  test "reports an unwrapped DDL string" do
    """
    defmodule Logflare.Repo.Migrations.DropAConstraint do
      use Ecto.Migration

      def up do
        execute("ALTER TABLE sources DROP CONSTRAINT sources_user_id_fkey")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "ALTER TABLE sources"))
  end

  test "reports an unwrapped DDL string built with interpolation" do
    ~S"""
    defmodule Logflare.Repo.Migrations.DropAnIndex do
      use Ecto.Migration

      @index_name :some_index

      def up do
        execute("DROP INDEX IF EXISTS #{@index_name}")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "DROP INDEX"))
  end

  test "ignores node-local statements and DML" do
    """
    defmodule Logflare.Repo.Migrations.NodeLocalThings do
      use Ecto.Migration

      def up do
        execute("CREATE PUBLICATION logflare_pub FOR TABLE sources;")
        execute("DROP PUBLICATION logflare_pub;")
        execute("ALTER TABLE sources REPLICA IDENTITY FULL")
        execute("ALTER SYSTEM SET wal_level = 'logical'")
        execute("UPDATE versions SET item_type = 'EndpointQuery'")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> refute_issues()
  end

  test "reports unwrapped DDL in the down SQL of execute/2" do
    """
    defmodule Logflare.Repo.Migrations.ReversibleExecute do
      use Ecto.Migration

      def change do
        execute("UPDATE sources SET name = name", "DROP TABLE sources_archive")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "DROP TABLE sources_archive"))
  end

  test "reports DDL on identifiers that merely contain a node-local keyword" do
    """
    defmodule Logflare.Repo.Migrations.CreatePublicationArchive do
      use Ecto.Migration

      def up do
        execute("CREATE TABLE publication_archive (id bigint)")
        execute("ALTER TABLE subscriptions ADD COLUMN plan text")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issues(&assert(length(&1) == 2))
  end

  test "reports DDL preceded by SQL comments" do
    ~S'''
    defmodule Logflare.Repo.Migrations.CommentedDdl do
      use Ecto.Migration

      def up do
        execute("""
        -- Remove old table
        DROP TABLE old_sources
        """)

        execute("/* create the new table */ CREATE TABLE new_sources (id bigint)")
      end
    end
    '''
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issues(&assert(length(&1) == 2))
  end

  test "reports DDL that follows another statement" do
    ~S"""
    defmodule Logflare.Repo.Migrations.SetThenCreate do
      use Ecto.Migration

      def up do
        execute("SET search_path = public; CREATE TABLE things (id bigint)")
        execute("SET search_path = #{prefix()}; CREATE TABLE other_things (id bigint)")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issues(&assert(length(&1) == 2))
  end

  test "reports DDL written with a string sigil" do
    """
    defmodule Logflare.Repo.Migrations.SigilDdl do
      use Ecto.Migration

      def up do
        execute(~s|ALTER TABLE sources ADD COLUMN "notes" text|)
        execute(~S(DROP INDEX sources_notes_index))
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issues(&assert(length(&1) == 2))
  end

  test "accepts DDL wrapped through a renamed alias or an import of the migrator" do
    """
    defmodule Logflare.Repo.Migrations.AliasedWrapper do
      use Ecto.Migration

      alias Logflare.Repo.Migrator, as: Replicated

      def up do
        Replicated.with_replicated_execute(fn ->
          execute("DROP TABLE old_sources")
        end)
      end
    end

    defmodule Logflare.Repo.Migrations.ImportedWrapper do
      use Ecto.Migration

      import Logflare.Repo.Migrator, only: [with_replicated_execute: 1]

      def up do
        with_replicated_execute(fn ->
          execute("DROP TABLE old_sources")
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> refute_issues()
  end

  test "reports DDL wrapped by a with_replicated_execute/1 from another module" do
    """
    defmodule Logflare.Repo.Migrations.WrongWrapper do
      use Ecto.Migration

      def up do
        Other.with_replicated_execute(fn ->
          execute("DROP TABLE old_sources")
        end)

        with_replicated_execute(fn ->
          execute("DROP TABLE older_sources")
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issues(&assert(length(&1) == 2))
  end

  test "resolves the migrator alias lexically at each call site" do
    """
    defmodule Logflare.Repo.Migrations.DropOldSources do
      use Ecto.Migration

      alias Logflare.Repo.Migrator

      def up do
        Migrator.with_replicated_execute(fn ->
          execute("DROP TABLE old_sources")
        end)
      end

      defp unrelated do
        alias Other.Migrator

        Migrator.with_replicated_execute(fn ->
          execute("DROP TABLE older_sources")
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "older_sources"))
  end

  test "reports DDL when the migrator alias is declared in another function" do
    """
    defmodule Logflare.Repo.Migrations.DropOldSources do
      use Ecto.Migration

      def up do
        alias Logflare.Repo.Migrator
        :ok
      end

      def down do
        Migrator.with_replicated_execute(fn ->
          execute("DROP TABLE old_sources")
        end)
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "old_sources"))
  end

  test "reports DDL that only shares a line with a wrapped block" do
    """
    defmodule Logflare.Repo.Migrations.SameLine do
      use Ecto.Migration

      alias Logflare.Repo.Migrator

      def up do
        Migrator.with_replicated_execute(fn -> :ok end); execute("DROP TABLE old_sources")
      end
    end
    """
    |> to_source_file()
    |> run_check(ReplicatedDdlExecute)
    |> assert_issue(&assert(&1.message =~ "old_sources"))
  end
end
