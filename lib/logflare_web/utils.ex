defmodule LogflareWeb.Utils do
  @moduledoc false
  alias Logflare.User

  @doc """
  Checks if a feature flag is enabled.
  If SDK key is not set, will always return false.
  In test mode, will always return true.

  ### Example
    iex> flag("my-feature")
    true
  """
  def flag(feature, user \\ nil) when is_binary(feature) do
    config_cat_key = Application.get_env(:logflare, :config_cat_sdk_key)
    env = Application.get_env(:logflare, :env)
    overrides = Application.get_env(:logflare, :feature_flag_override, %{})

    cond do
      env == :test ->
        true

      config_cat_key != nil ->
        case user do
          nil ->
            ConfigCat.get_value(feature, false)

          %User{} ->
            user_obj = ConfigCat.User.new(user.email)
            ConfigCat.get_value("alerts", false, user_obj)
        end

      true ->
        Map.get(overrides, feature, "false") == "true"
    end
  end
end
