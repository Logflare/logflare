defmodule Logflare.Repo.Migrations.CreateTeamUsers do
  use Ecto.Migration

  def change do
    create table(:teams) do
      add :name, :string
      add :user_id, references(:users, on_delete: :delete_all)

      timestamps()
    end

    create table(:team_users) do
      add :email, :string
      add :token, :string
      add :provider, :string
      add :email_preferred, :string
      add :name, :string
      add :image, :string
      add :email_me_product, :boolean, default: false, null: false
      add :phone, :string
      add :valid_google_account, :boolean, default: false, null: false
      add :provider_uid, :string
      add :team_id, references(:teams, on_delete: :delete_all)

      timestamps()
    end

    alter table(:users) do
      add(:company, :string)
    end

    create index(:team_users, [:team_id])
    create unique_index(:team_users, [:provider_uid, :team_id])
    create unique_index(:teams, [:user_id])
  end
end
