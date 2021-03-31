defmodule Logflare.Repo.Migrations.RenameColumnsForMnesiaCompatibility do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :billing_enabled, :boolean, null: false, default: false
    end

    execute(~s|UPDATE users SET billing_enabled = "billing_enabled?"|)

    if Mix.env in [:dev, :test] do
      alter table(:users) do
        remove :billing_enabled?, :boolean, null: false, default: false
      end
    end

    alter table(:saved_searches) do
      add :tailing, :boolean, null: false, default: true
    end

    execute(~s|UPDATE saved_searches SET tailing = "tailing?"|)

    if Mix.env in [:dev, :test] do
      alter table(:saved_searches) do
        remove :tailing?, :boolean, null: false, default: true
      end
    end

    alter table(:billing_accounts) do
      add :lifetime_plan, :boolean, null: false, default: false
    end

    execute(~s|UPDATE billing_accounts SET lifetime_plan = "lifetime_plan?"|)


    if Mix.env in [:dev, :test] do
      alter table(:billing_accounts) do
        remove :lifetime_plan?, :boolean, null: false, default: false
      end
    end

  end
end
