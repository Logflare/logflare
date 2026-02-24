defmodule LogflareWeb.Utils do
  @moduledoc false

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
      Enum.reduce_while(units, count, fn unit, size ->
        if size >= 1024 do
          {:cont, size / 1024}
        else
          {:halt, {size * 1.0, unit}}
        end
      end)

    {Float.round(size, 2), unit}
  end

  @spec sql_params_to_sql(String.t(), list()) :: String.t()
  def sql_params_to_sql(sql, params) when is_binary(sql) and is_list(params) do
    Enum.reduce(params, sql, fn param, acc_sql ->
      type = Map.get(param.parameterType, :type)
      value = Map.get(param.parameterValue, :value)

      replacement =
        case type do
          "STRING" -> "'#{value}'"
          num when num in ["INTEGER", "FLOAT"] -> inspect(value)
          _ -> inspect(value)
        end

      String.replace(acc_sql, "?", replacement, global: false)
    end)
  end

  @spec replace_table_with_source_name(String.t(), %{
          bq_table_id: String.t(),
          name: String.t()
        }) :: String.t()
  def replace_table_with_source_name(sql, %{bq_table_id: table_id, name: name})
      when is_binary(sql) and is_binary(table_id) and is_binary(name) do
    quoted_name = "`#{name}`"

    table_variants =
      [table_id, String.replace(table_id, "`", "")]
      |> Enum.filter(&(&1 != ""))
      |> Enum.uniq()

    Enum.reduce(table_variants, sql, fn variant, acc ->
      String.replace(acc, variant, quoted_name)
    end)
  end

  def replace_table_with_source_name(sql, _source), do: sql

  @doc """
  Add a team_id param to a URI.
  Team id can be passed as an integer, a conn with a team_id param, or nil (no team_id will be set)
  An existing team_id param will be overwritten.

  Use team_link/1 in HEEx templates.

  ## Examples

      iex> with_team_param("/dashboard", 123)
      "/dashboard?t=123"

      iex> with_team_param("/dashboard?t=999", 123)
      "/dashboard?t=123"

      iex> team = %Logflare.Teams.Team{id: 123}
      iex> with_team_param("/dashboard", team)
      "/dashboard?t=123"

      iex> with_team_param("/dashboard", nil)
      "/dashboard"

      iex> with_team_param("/dashboard", nil)
      "/dashboard"

  """
  @spec with_team_param(String.t(), nil | integer() | Phoenix.Param.t()) :: String.t()
  def with_team_param(uri, nil), do: uri

  def with_team_param(uri, team) when is_struct(team),
    do: with_team_param(uri, Phoenix.Param.to_param(team))

  def with_team_param(uri, team_id) when is_binary(team_id) do
    case Integer.parse(team_id) do
      {int_team_id, ""} -> with_team_param(uri, int_team_id)
      _ -> uri
    end
  end

  def with_team_param(uri, team_id) when is_integer(team_id) do
    uri
    |> URI.parse()
    |> set_team_id_param(team_id)
  end

  defp set_team_id_param(uri, team_id) do
    query_params =
      (uri.query || "")
      |> URI.decode_query()
      |> Map.put("t", team_id)
      |> URI.encode_query()

    %{uri | query: query_params}
    |> URI.to_string()
  end
end
