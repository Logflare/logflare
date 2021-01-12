defmodule Logflare.Repo.Migrations.AlterTableUserRemoveQuestionMarks do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :billing_enabled, :boolean
    end

    execute(
    """
    UPDATE users
    SET billing_enabled = "billing_enabled?"
    """)

    alter table(:billing_accounts) do
      add :lifetime_plan, :boolean, default: false, nullable: false
    end

    execute(
    """
    UPDATE billing_accounts
    SET lifetime_plan = "lifetime_plan?"
    """)

    alter table(:saved_searches) do
      add :tailing, :boolean, null: false, default: true
    end

    execute(
    """
    UPDATE saved_searches
    SET tailing = "tailing?"
    """)
  end
end
