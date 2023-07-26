defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations.AddLogEvents do
  @moduledoc """
  Migration to generate a log_events table for a given source_id
  """
  use Ecto.Migration

  alias Logflare.Backends.SourceBackend
  alias Logflare.Backends.Adaptor.PostgresAdaptor.PgRepo

  @doc """
  Generates a Log Event table for a given Source Backend.

  The table name is generated from the source token associated with the source backend.
  """
  def generate_migration(%SourceBackend{source: %{token: token}} = source_backend) do
    token = token |> Atom.to_string() |> String.replace("-", "")
    name = Module.concat([__MODULE__, "AddLogEventsForSource#{token}"])
    table_name = PgRepo.table_name(source_backend)

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
