defmodule Logflare.Repo.Migrations.AddV2PipelineColumnToSourceTable do
  use Ecto.Migration

  def change do
    alter table "sources" do
      add :v2_pipeline, :boolean, default: false
    end
  end
end
