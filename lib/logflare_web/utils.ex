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
  Converts changeset errors to a human-readable string format. There's an optional prefix message you can provide as the second argument.

  ## Examples

      iex> changeset = %Ecto.Changeset{
      ...>   errors: [name: {"can't be blank", []}, email: {"is invalid", []}],
      ...>   data: %{},
      ...>   types: %{name: :string, email: :string}
      ...> }
      iex> stringify_changeset_errors(changeset)
      "name: can't be blank\\nemail: is invalid"

      iex> changeset = %Ecto.Changeset{
      ...>   errors: [age: {"must be greater than %{number}", [number: 18]}],
      ...>   data: %{},
      ...>   types: %{age: :integer}
      ...> }
      iex> stringify_changeset_errors(changeset)
      "age: must be greater than 18"

      iex> changeset = %Ecto.Changeset{
      ...>   errors: [],
      ...>   data: %{},
      ...>   types: %{}
      ...> }
      iex> stringify_changeset_errors(changeset)
      ""

      iex> changeset = %Ecto.Changeset{
      ...>   errors: [name: {"can't be blank", []}],
      ...>   data: %{},
      ...>   types: %{name: :string}
      ...> }
      iex> stringify_changeset_errors(changeset, "Validation failed")
      "Validation failed: name: can't be blank"

      iex> changeset = %Ecto.Changeset{
      ...>   errors: [],
      ...>   data: %{},
      ...>   types: %{}
      ...> }
      iex> stringify_changeset_errors(changeset, "No errors")
      "No errors"

  """
  def stringify_changeset_errors(%Ecto.Changeset{} = changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
      Enum.reduce(opts, msg, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", _to_string(value))
      end)
    end)
    |> Enum.reduce([], fn {k, v}, acc ->
      ["#{k}: #{Enum.join(v, " & ")}" | acc]
    end)
    |> Enum.reverse()
    |> Enum.join("\n")
  end

  def stringify_changeset_errors(%Ecto.Changeset{} = changeset, default_message) do
    changeset
    |> Ecto.Changeset.traverse_errors(fn {msg, _opts} -> msg end)
    |> Enum.map(fn {field, errors} -> "#{field}: #{_to_string(errors)}" end)
    |> Enum.join("; ")
    |> case do
      "" -> default_message
      errors -> "#{default_message}: #{errors}"
    end
  end

  defp _to_string(val) when is_list(val) do
    Enum.join(val, ", ")
  end

  defp _to_string(val), do: to_string(val)
end
