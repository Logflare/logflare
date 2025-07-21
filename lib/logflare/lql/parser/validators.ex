defmodule Logflare.Lql.Parser.Validators do
  @moduledoc """
  Validation functions for parsed values
  """

  @spec check_for_no_invalid_metadata_field_values(map(), :timestamp | :metadata) ::
          map() | no_return()
  def check_for_no_invalid_metadata_field_values(
        %{path: _p, value: {:invalid_metadata_field_value, v}},
        :timestamp
      ) do
    throw(
      "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got '#{v}'"
    )
  end

  def check_for_no_invalid_metadata_field_values(
        %{path: p, value: {:invalid_metadata_field_value, v}},
        :metadata
      ) do
    throw("Error while parsing `#{p}` field metadata filter value: #{v}")
  end

  def check_for_no_invalid_metadata_field_values(rule, _), do: rule
end
