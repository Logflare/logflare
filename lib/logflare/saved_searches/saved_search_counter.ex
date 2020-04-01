defmodule Logflare.SavedSearchCounter do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset

  typed_schema "saved_search_counters" do
    field :datetime, :utc_datetime
    belongs_to :saved_search, SavedSearch
    field :granularity, :string, default: "day"
    field :tailing_count, :integer
    field :non_tailing_count, :integer
  end

  def changeset(counter, attrs \\ %{}) do
    counter
    |> cast(attrs, [:datetime])
    |> unique_constraint(:datetime, name: :saved_search_counters_datetime_source_id_granularity)
  end
end
