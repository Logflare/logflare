defmodule Logflare.Repo.Migrations.AddCustomEventMessage do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      add :custom_event_message_keys, :string
    end
  end
end
