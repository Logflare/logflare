defmodule Logflare.Repo.Migrations.AddRegexCompiledToRules do
  use Ecto.Migration

  def change do
    alter table(:rules) do
      add_if_not_exists :regex_struct, :bytea
    end
  end
end

