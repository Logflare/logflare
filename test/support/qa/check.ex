defmodule Logflare.QA.Check do
  @moduledoc "The result of one QA check, returned by code that runs on the server node."

  @enforce_keys [:label, :ok?]
  defstruct [:label, :ok?, detail: nil]

  @type t :: %__MODULE__{label: String.t(), ok?: boolean(), detail: String.t() | nil}
end
