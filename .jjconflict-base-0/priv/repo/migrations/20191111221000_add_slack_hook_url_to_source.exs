defmodule Logflare.Repo.Migrations.AddSlackHookUrlToSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add(:slack_hook_url, :string)
    end
  end
end
