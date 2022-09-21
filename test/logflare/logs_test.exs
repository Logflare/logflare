defmodule Logflare.LogsTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{Logs.IngestTransformers}

  describe "ingest transformers" do
    test "transform/2 with :field_length" do
      for {input_n, expected_n, max_n, starts_with_underscore?} <- [
            # TODO: Verify that underscore prefixing resulting in exceeding total n is expected.
            {130, 101, 100, true},
            {50, 50, 100, false}
          ] do
        key = String.duplicate("f", input_n)

        params = log_params_fixture(key)

        assert %{"metadata" => meta} =
                 IngestTransformers.transform(
                   params,
                   [{:field_length, [max: max_n]}]
                 )

        for transformed_key <- Map.keys(meta) do
          assert String.length(transformed_key) === expected_n
          assert String.starts_with?(transformed_key, "_") == starts_with_underscore?
        end
      end
    end

    test "transform/2 with alphanumeric_only" do
      params = log_params_fixture("@somekey123%testing$")

      assert %{"metadata" => meta} = IngestTransformers.transform(params, [:alphanumeric_only])
      assert [key] = Map.keys(meta)
      assert String.starts_with?(key, "__")
      assert String.ends_with?(key, "_")
      assert key =~ "3_testing"
      refute key =~ "$" and key =~ "@" and key =~ "%"
    end

    test "transform/2 with strip_bq_prefixes" do
      for {input_key, expected} <- [
            {"_TABLE_testing", "__TABLE_testing"},
            {"_PARTITION_testing", "__PARTITION_testing"},
            {"_FILE_testing", "__FILE_testing"},
            {"_testing", "_testing"}
          ],
          params = log_params_fixture(input_key) do
        assert %{"metadata" => meta} = IngestTransformers.transform(params, [:strip_bq_prefixes])
        assert [key] = Map.keys(meta)
        # TODO: Check with chase if this is expected behaviour
        assert key == expected
      end
    end

    test "transform/2 with dashes_to_underscores" do
      params = log_params_fixture("some-test")

      assert %{"metadata" => meta} =
               IngestTransformers.transform(params, [:dashes_to_underscores])

      assert [key] = Map.keys(meta)
      assert key == "_some_test"
    end

    test "transform/2 with alter_leading_numbers" do
      params = log_params_fixture("1test")

      assert %{"metadata" => meta} =
               IngestTransformers.transform(params, [:alter_leading_numbers])

      assert [key] = Map.keys(meta)
      assert key == "_1test"
    end

    test "non-map datatypes" do
      params = %{"metadata" => "some string"}
      assert ^params = IngestTransformers.transform(params, :to_bigquery_column_spec)

      params = %{"metadata" => ["some string"]}
      assert ^params = IngestTransformers.transform(params, :to_bigquery_column_spec)

      params = %{"metadata" => [%{"some" => "some string"}]}
      assert ^params = IngestTransformers.transform(params, :to_bigquery_column_spec)

      params = %{"metadata" => nil}
      assert ^params = IngestTransformers.transform(params, :to_bigquery_column_spec)
    end

    defp log_params_fixture(key),
      do: %{"metadata" => %{key => "value"}}
  end
end
