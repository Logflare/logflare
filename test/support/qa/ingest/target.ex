defmodule Logflare.QA.Ingest.Target do
  @moduledoc """
  A routing target of the ingest QA source: a sink source behind a source rule, or a
  Postgres backend behind a backend rule. `expect` lists the event kinds it must receive.
  """

  @enforce_keys [:type, :name, :lql, :expect]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          type: :source | :backend,
          name: String.t(),
          lql: String.t(),
          expect: [String.t()]
        }
end
