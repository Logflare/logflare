defmodule Logflare.Backends.Adaptor.PostgresAdaptor.LogEvent do
  @moduledoc """
  Logflare Log Event schema to be used by the Postgres Adaptor
  """
  use Ecto.Schema
  import Ecto.Changeset
  @primary_key {:id, :string, []}
  schema "log_events" do
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
