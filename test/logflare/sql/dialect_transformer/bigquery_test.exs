defmodule Logflare.Sql.DialectTransformer.BigQueryTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.BigQuery
  alias Logflare.User

  @logflare_project_id "logflare-project-id"
  @user_project_id "user-project-id"
  @user_dataset_id "user-dataset-id"

  setup do
    build(:plan)
    values = Application.get_env(:logflare, Logflare.Google)
    to_put = Keyword.put(values, :project_id, @logflare_project_id)
    Application.put_env(:logflare, Logflare.Google, to_put)

    on_exit(fn ->
      Application.put_env(:logflare, Logflare.Google, values)
    end)
  end

  describe "quote_style/0" do
    test "returns backticks for BigQuery" do
      assert BigQuery.quote_style() == "`"
    end
  end

  describe "dialect/0" do
    test "returns bigquery string" do
      assert BigQuery.dialect() == "bigquery"
    end
  end

  describe "transform_source_name/2" do
    test "uses logflare project when user project is nil" do
      user = build(:user, bigquery_project_id: nil, bigquery_dataset_id: nil)
      source = build(:source, name: "test_source", user: user)

      data = %{
        sources: [source],
        logflare_project_id: @logflare_project_id,
        user_project_id: nil,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: nil
      }

      result = BigQuery.transform_source_name("test_source", data)
      expected_token = Atom.to_string(source.token) |> String.replace("-", "_")
      assert result == "#{@logflare_project_id}.logflare_dataset.#{expected_token}"
    end

    test "uses user project when configured" do
      user =
        build(:user,
          bigquery_project_id: @user_project_id,
          bigquery_dataset_id: @user_dataset_id
        )

      source = build(:source, name: "test_source", user: user)

      data = %{
        sources: [source],
        logflare_project_id: @logflare_project_id,
        user_project_id: @user_project_id,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: @user_dataset_id
      }

      result = BigQuery.transform_source_name("test_source", data)
      expected_token = Atom.to_string(source.token) |> String.replace("-", "_")
      assert result == "#{@user_project_id}.#{@user_dataset_id}.#{expected_token}"
    end

    test "sanitizes hyphens in tokens" do
      user = build(:user, bigquery_project_id: nil, bigquery_dataset_id: nil)
      source = build(:source, name: "hyphenated_source", user: user)

      data = %{
        sources: [source],
        logflare_project_id: @logflare_project_id,
        user_project_id: nil,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: nil
      }

      result = BigQuery.transform_source_name("hyphenated_source", data)
      expected_token = Atom.to_string(source.token) |> String.replace("-", "_")
      assert result == "#{@logflare_project_id}.logflare_dataset.#{expected_token}"
    end

    test "finds correct source by name" do
      user = build(:user, bigquery_project_id: nil, bigquery_dataset_id: nil)
      source1 = build(:source, name: "source_one", user: user)
      source2 = build(:source, name: "source_two", user: user)

      data = %{
        sources: [source1, source2],
        logflare_project_id: @logflare_project_id,
        user_project_id: nil,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: nil
      }

      result = BigQuery.transform_source_name("source_two", data)
      expected_token = Atom.to_string(source2.token) |> String.replace("-", "_")
      assert result == "#{@logflare_project_id}.logflare_dataset.#{expected_token}"
    end
  end

  describe "validate_transformation_data/1" do
    test "validates when both user project and dataset are nil" do
      data = %{
        logflare_project_id: @logflare_project_id,
        user_project_id: nil,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: nil
      }

      assert BigQuery.validate_transformation_data(data) == :ok
    end

    test "validates when both user project and dataset are present" do
      data = %{
        logflare_project_id: @logflare_project_id,
        user_project_id: @user_project_id,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: @user_dataset_id
      }

      assert BigQuery.validate_transformation_data(data) == :ok
    end

    test "fails when only user project is set" do
      data = %{
        logflare_project_id: @logflare_project_id,
        user_project_id: @user_project_id,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: nil
      }

      assert {:error, "Invalid BigQuery project/dataset configuration"} =
               BigQuery.validate_transformation_data(data)
    end

    test "fails when only user dataset is set" do
      data = %{
        logflare_project_id: @logflare_project_id,
        user_project_id: nil,
        logflare_dataset_id: "logflare_dataset",
        user_dataset_id: @user_dataset_id
      }

      assert {:error, "Invalid BigQuery project/dataset configuration"} =
               BigQuery.validate_transformation_data(data)
    end

    test "fails when missing required fields" do
      data = %{user_project_id: @user_project_id}

      assert {:error, "Missing BigQuery transformation data"} =
               BigQuery.validate_transformation_data(data)
    end
  end

  describe "build_transformation_data/2" do
    test "builds data with user's BigQuery configuration" do
      user =
        build(:user,
          bigquery_project_id: @user_project_id,
          bigquery_dataset_id: @user_dataset_id
        )

      base_data = %{
        sources: [],
        source_mapping: %{},
        dialect: "bigquery"
      }

      result = BigQuery.build_transformation_data(user, base_data)

      assert result.logflare_project_id == @logflare_project_id
      assert result.user_project_id == @user_project_id
      assert result.user_dataset_id == @user_dataset_id
      assert result.logflare_dataset_id == User.generate_bq_dataset_id(user)
      assert result.sources == []
      assert result.source_mapping == %{}
      assert result.dialect == "bigquery"
    end

    test "preserves base data fields" do
      user = build(:user, bigquery_project_id: nil, bigquery_dataset_id: nil)

      base_data = %{
        sources: [:source1],
        source_mapping: %{"test" => "mapping"},
        dialect: "bigquery",
        custom_field: "preserved"
      }

      result = BigQuery.build_transformation_data(user, base_data)

      assert result.sources == [:source1]
      assert result.source_mapping == %{"test" => "mapping"}
      assert result.custom_field == "preserved"
    end
  end
end
