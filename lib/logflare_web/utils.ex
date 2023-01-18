defmodule LogflareWeb.Utils do
  @moduledoc false

  @doc """
  Checks if a feature flag is enabled.

  ### Example
    iex> flag("my-feature")
    true
  """
  def flag(feature) when is_binary(feature) do
    ConfigCat.get_value(feature, false)
  end
end
