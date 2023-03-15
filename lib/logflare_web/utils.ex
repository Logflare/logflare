defmodule LogflareWeb.Utils do
  @moduledoc false

  @doc """
  Checks if a feature flag is enabled.
  If SDK key is not set, will always return false.
  In test mode, will always return true.

  ### Example
    iex> flag("my-feature")
    true
  """
  def flag(feature) when is_binary(feature) do
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)
    env = Application.get_env(:logflare, :env)

    cond do
      env == :test ->
        true

      config_cat_key != nil ->
        ConfigCat.get_value(feature, false)

      true ->
        false
    end
  end
end
