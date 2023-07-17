defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations.AddLogEvents do
  @moduledoc """
  Migration to generate a log_events table for a given source_id
  """
  use Ecto.Migration

  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo

  def generate_migration(source_backend) do
    table_name = PgRepo.table_name(source_backend)
    name = Module.concat([__MODULE__, "MigrationFor#{table_name}"])

    ast =
      quote do
        use Ecto.Migration

        def up do
          create table(unquote(table_name), primary_key: false) do
            add(:id, :string, primary_key: true)
            add(:body, :map)
            add(:event_message, :string)
            add(:timestamp, :utc_datetime_usec)
          end
        end

        def down do
          drop(table(unquote(table_name)))
        end
      end

    case Code.ensure_compiled(name) do
      {:module, _} -> nil
      _ -> {:module, _, _, _} = Module.create(name, ast, Macro.Env.location(__ENV__))
    end

    name
  end
end
