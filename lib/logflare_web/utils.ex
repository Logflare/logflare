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
            ConfigCat.get_value(feature, false, user_obj)
        end

      true ->
        Map.get(overrides, feature, "false") == "true"
    end
  end

  @doc """
  Converts a bytes count to human readable scale.

  ## Examples

    iex> humanize_bytes(1)
    {1.0, "byte"}

    iex> humanize_bytes(2)
    {2.0, "bytes"}

    iex> humanize_bytes(1024)
    {1.0, "KB"}

    iex> humanize_bytes(1363149)
    {1.3, "MB"}

    iex> humanize_bytes(50 * 1024 * 1024 * 1024)
    {50.0, "GB"}

    iex> humanize_bytes(2 * 1024 * 1024 * 1024 * 1024)
    {2.0, "TB"}

  """
  @spec humanize_bytes(integer) :: {float(), String.t()}
  def humanize_bytes(count) when count == 1, do: {1.0, "byte"}

  def humanize_bytes(count) when is_integer(count) do
    units = ["bytes", "KB", "MB", "GB", "TB"]

    {size, unit} =
      Enum.reduce_while(units, {count, 1}, fn unit, {size, index} ->
        if size >= 1024 do
          {:cont, {size / 1024, index + 1}}
        else
          {:halt, {size * 1.0, unit}}
        end
      end)

    {Float.round(size, 2), unit}
  end
end
