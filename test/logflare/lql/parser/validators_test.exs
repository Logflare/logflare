defmodule Logflare.Lql.Parser.ValidatorsTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.Parser.Validators

  describe "check_for_no_invalid_metadata_field_values/2" do
    test "throws error for invalid timestamp value" do
      rule = %{path: "timestamp", value: {:invalid_metadata_field_value, "invalid-timestamp"}}

      assert catch_throw(Validators.check_for_no_invalid_metadata_field_values(rule, :timestamp)) ==
               "Error while parsing timestamp filter value: expected ISO8601 string or range or shorthand, got 'invalid-timestamp'"
    end

    test "throws error for invalid metadata field value" do
      rule = %{path: "metadata.level", value: {:invalid_metadata_field_value, "bad-value"}}

      assert catch_throw(Validators.check_for_no_invalid_metadata_field_values(rule, :metadata)) ==
               "Error while parsing `metadata.level` field metadata filter value: bad-value"
    end

    test "passes through valid rule unchanged" do
      rule = %{path: "timestamp", value: ~N[2023-01-01 12:00:00]}
      result = Validators.check_for_no_invalid_metadata_field_values(rule, :timestamp)

      assert result == rule
    end

    test "passes through valid metadata rule unchanged" do
      rule = %{path: "metadata.level", value: "info"}
      result = Validators.check_for_no_invalid_metadata_field_values(rule, :metadata)

      assert result == rule
    end
  end
end
