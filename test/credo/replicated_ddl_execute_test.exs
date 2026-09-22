defmodule Logflare.CredoChecks.ReplicatedDdlExecuteTest do
  use Credo.Test.Case

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
end
