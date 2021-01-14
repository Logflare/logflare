defmodule Logflare.RejectedLogEvent do
  use Logflare.Commons
  use TypedEctoSchema

  schema "rejected_log_events" do
    field :params, :map
    field :validation_error, :string
    field :ingested_at, :utc_datetime_usec
    belongs_to :source, Source
  end

  def changefeed_changeset(attrs) do
    EctoChangesetExtras.cast_all_fields(%__MODULE__{}, attrs)
  end
end
