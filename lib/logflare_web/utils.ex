defmodule LogflareWeb.Utils do
  @moduledoc false

  @doc """
  Checks if a feature flag is enabled.

  ### Example
    iex> flag("my-feature")
    true
  """
  def flag(feature) when is_binary(feature) do
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)

    if config_cat_key do
      ConfigCat.get_value(feature, false)
    else
      false
    end
  end
end
