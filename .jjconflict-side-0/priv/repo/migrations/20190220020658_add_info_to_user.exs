defmodule Logflare.Repo.Migrations.AddInfoToUser do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add(:email_preferred, :string)
      add(:name, :string)
      add(:image, :string)
      add(:email_me_product, :boolean, default: true, null: false)
    end
  end
end
