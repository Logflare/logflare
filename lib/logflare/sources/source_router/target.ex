defmodule Logflare.Sources.SourceRouter.Target do
  @moduledoc false

  alias Logflare.Rules.Rule

  @type t() :: {Rule.id(), backend_id :: non_neg_integer() | nil, sink :: atom() | nil}

  @spec from_rule(Rule.t()) :: t()
  def from_rule(%Rule{id: id, backend_id: backend_id, sink: sink}),
    do: {id, backend_id, sink}

  @spec id(t()) :: Rule.id()
  def id({id, _backend_id, _sink}), do: id
end
