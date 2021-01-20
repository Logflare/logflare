defmodule Logflare.RejectedLogEvent do
  use Logflare.Commons
  use TypedEctoSchema
  use Logflare.ChangefeedSchema

  schema "rejected_log_events" do
    field :params, :map
    field :validation_error, :string
    field :ingested_at, :utc_datetime_usec
    belongs_to :source, Source
  end

  use Logflare.ChangefeedSchema
end
