defmodule Logflare.Support.PglogicalTestMigration do
  @moduledoc false
  use Ecto.Migration

  def up do
    create table(:pglogical_ddl_test) do
      add(:name, :string)
    end
  end

  def down do
    drop(table(:pglogical_ddl_test))
  end
end
