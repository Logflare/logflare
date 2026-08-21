defmodule Logflare.NaturalLanguageLql.Client do
  @moduledoc false

  @callback generate(String.t()) ::
              {:ok, %{required(:text) => String.t(), optional(:request_id) => String.t() | nil}}
              | {:error, :unavailable}
end
