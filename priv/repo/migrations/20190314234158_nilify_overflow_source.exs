defmodule Logflare.Repo.Migrations.NilifyOverflowSource do
  use Ecto.Migration

  def change do
    alter table(:sources) do
      remove(:overflow_source)
    end

    alter table(:sources) do
      add(
        :overflow_source,
        references(:sources, column: :token, type: :uuid, on_delete: :nilify_all)
      )
    end
  end
end
