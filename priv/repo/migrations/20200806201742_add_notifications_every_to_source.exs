defmodule Logflare.Repo.Migrations.AddNotificationsEveryToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :notifications_every, :integer, default: :timer.hours(4), nullable: false
    end
  end
end
