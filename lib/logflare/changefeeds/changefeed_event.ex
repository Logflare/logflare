defmodule Logflare.Changefeeds.ChangefeedEvent do
  use Logflare.Commons
  use TypedStruct

  typedstruct do
    field :id, term(), require: true
    field :node_id, atom(), require: true
    field :changes, map() | nil
    field :table, String.t(), require: true
    field :type, atom(), require: true
    field :changefeed_subscription, Changefeeds.ChangefeedSubscription.t()
  end

  def build(attrs) do
    kvs =
      attrs
      |> Enum.map(fn {k, v} ->
        {String.to_atom(k), v}
      end)
      |> Enum.map(fn
        {:id, v} -> {:id, v}
        {:type, v} -> {:type, v |> String.downcase() |> String.to_existing_atom()}
        {k, v} -> {k, v}
      end)

    kvs = Keyword.update!(kvs, :node_id, &String.to_atom/1)

    struct!(
      __MODULE__,
      kvs ++
        [
          changefeed_subscription: Changefeeds.get_changefeed_subscription_by_table(kvs[:table])
        ]
    )
  end
end
