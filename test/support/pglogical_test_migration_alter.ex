defmodule Logflare.Support.PglogicalTestMigrationAlter do
  @moduledoc false
  use Ecto.Migration

  def up do
    alter table(:pglogical_ddl_test) do
      add(:extra_column, :string)
    end
  end

  def down do
    alter table(:pglogical_ddl_test) do
      remove(:extra_column)
    end
  end
end
