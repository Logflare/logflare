defmodule Logflare.Repo.MigratorTest do
  use ExUnit.Case, async: false

  alias Logflare.Repo.Migrator

  setup do
    previous = Application.get_env(:logflare, Migrator)

    on_exit(fn ->
      if previous do
        Application.put_env(:logflare, Migrator, previous)
      else
        Application.delete_env(:logflare, Migrator)
      end
    end)

    :ok
  end

  describe "migration_repo_for/1" do
    test "returns Logflare.Repo when replication_sets is empty" do
      Application.put_env(:logflare, Migrator, replication_sets: [])

      assert Migrator.migration_repo_for(Logflare.Repo) == Logflare.Repo
    end

    test "returns Logflare.Repo when replication_sets is not configured" do
      Application.delete_env(:logflare, Migrator)

      assert Migrator.migration_repo_for(Logflare.Repo) == Logflare.Repo
    end

    test "returns Logflare.Repo.Pglogical when replication_sets is non-empty" do
      Application.put_env(:logflare, Migrator, replication_sets: ["my_set"])

      assert Migrator.migration_repo_for(Logflare.Repo) == Logflare.Repo.Pglogical
    end

    test "passes through non-primary repos unchanged" do
      Application.put_env(:logflare, Migrator, replication_sets: ["my_set"])

      assert Migrator.migration_repo_for(SomeOtherRepo) == SomeOtherRepo
    end
  end

  describe "replication_sets/0" do
    test "returns empty list when unconfigured" do
      Application.delete_env(:logflare, Migrator)

      assert Migrator.replication_sets() == []
    end

    test "returns configured replication sets" do
      Application.put_env(:logflare, Migrator, replication_sets: ["a", "b"])

      assert Migrator.replication_sets() == ["a", "b"]
    end
  end

  describe "with_replicated_execute/1" do
    test "is disabled by default" do
      refute Migrator.replicate_execute?()
    end

    test "enables the flag inside the block and restores it after" do
      assert Migrator.with_replicated_execute(fn ->
               assert Migrator.replicate_execute?()
               :done
             end) == :done

      refute Migrator.replicate_execute?()
    end

    test "does not flush when called outside a migration runner" do
      refute Process.get(:ecto_migration)

      assert Migrator.with_replicated_execute(fn -> :ok end) == :ok
    end

    test "restores the flag when the block raises" do
      assert_raise RuntimeError, fn ->
        Migrator.with_replicated_execute(fn -> raise "boom" end)
      end

      refute Migrator.replicate_execute?()
    end

    test "supports nesting" do
      Migrator.with_replicated_execute(fn ->
        Migrator.with_replicated_execute(fn -> assert Migrator.replicate_execute?() end)

        assert Migrator.replicate_execute?()
      end)

      refute Migrator.replicate_execute?()
    end
  end
end
