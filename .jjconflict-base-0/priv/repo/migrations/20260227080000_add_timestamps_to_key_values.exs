defmodule Logflare.Repo.Migrations.AddTimestampsToKeyValues do
  use Ecto.Migration

  def change do
    alter table(:key_values) do
      timestamps(type: :utc_datetime_usec, default: fragment("now()"))
    end
  end
end
