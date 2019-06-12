defmodule Logflare.Logs.LogEvent do
  use TypedStruct

  typedstruct do
    field :body, map, enforce: true
    field :valid, boolean
    field :validation_error, String.t()
  end
end
