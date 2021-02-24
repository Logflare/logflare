defmodule Logflare.Changefeeds.ChangefeedSubscription do
  use TypedStruct

  typedstruct do
    field :table, String.t(), require: true
    field :schema, module(), require: true
    field :id_only, boolean, require: true
    field :channel, String.t(), require: true
  end
end
