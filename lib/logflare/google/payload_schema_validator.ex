defmodule Logflare.Google.BigQuery.EventUtils.Validator do
  import Ecto.Changeset

  @doc """
  Validates incoming event payload to match the BigQuery schema requirements, which are the following:

  A column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
  and it must start with a letter or underscore.

  The maximum column name length is 128 characters.

  A column name cannot use any of the following prefixes:

  _TABLE_
  _FILE_
  _PARTITION

  Duplicate column names are not allowed even if the case differs. For example, a column named Column1 is considered identical to a column named column1.

  Source: https://cloud.google.com/bigquery/docs/schemas
  """
  defguard is_enum?(v) when is_map(v) or is_list(v)

  def valid?(payload) when is_map(payload) do
    Enum.reduce(
      payload,
      keys_valid?(payload),
      fn
        {_k, v}, acc when is_enum?(v) -> acc && valid?(v)
        _, acc -> acc
      end
    )
  end

  def valid?(payload) when is_list(payload) do
    Enum.reduce(payload, true, &(&2 && valid?(&1)))
  end

  defp keys_valid?(payload) do
    initial = %{keys: [], params: %{}, types: %{}}

    %{keys: keys, params: params, types: types} =
      Enum.reduce(payload, initial, fn {key, _}, acc ->
        keyatom = String.to_atom(key)

        acc
        |> Map.update!(:keys, &[keyatom | &1])
        |> Map.update!(:params, &put_in(&1, [keyatom], key))
        |> Map.update!(:types, &put_in(&1, [keyatom], :string))
      end)

    changeset = cast({%{}, types}, params, keys)

    keys_valid =
      Enum.reduce(params, changeset, fn {key, _}, acc -> apply_validations(acc, key) end).valid?

    keys_valid && downcase_keys_unique?(payload)
  end

  defp downcase_keys_unique?(payload) do
    keys =
      payload
      |> Map.keys()
      |> Enum.sort()

    keys === Enum.uniq_by(keys, &String.downcase(&1, :ascii))
  end

  defp apply_validations(changeset, key) do
    changeset
    # validates that the key starts from ascii letter or underscore
    |> validate_format(key, ~r/^[a-zA-Z_].*$/)
    # validates that the key starts from ascii letter or underscore
    |> validate_format(key, ~r/^[a-zA-Z0-9_]*$/)
    # validates that the key is not longer than 128 symbols
    |> validate_length(key, max: 128, min: 1)
    # validates that the key doesn't start from a reserved prefix using
    # a negative lookahead regex
    |> validate_format(key, ~r/^(?![_TABLE_|_FILE_|_PARTITION_]).*/)
  end
end
