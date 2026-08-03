defmodule Logflare.Backends.Adaptor.PostgresAdaptor.PgLogEvent do
  @moduledoc """
  Logflare Log Event schema to be used by the Postgres Adaptor
  """
  use TypedEctoSchema
  import Ecto.Changeset

  @primary_key {:id, :string, []}
  typed_schema "log_event" do
    field(:body, :map)
    field(:event_message, :string)
    field(:timestamp, :utc_datetime_usec)
  end

  def changeset(log_event, attrs) do
    log_event
    |> cast(attrs, [:id, :body, :timestamp, :event_message])
    |> validate_required([:id, :timestamp, :event_message])
  end
end
