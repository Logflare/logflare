defmodule Logflare.Google.BigQueryTest do
  use Logflare.DataCase

  alias GoogleApi.BigQuery.V2.Api.Tables
  alias GoogleApi.BigQuery.V2.Model.Streamingbuffer
  alias GoogleApi.BigQuery.V2.Model.Table
  alias Logflare.Google.BigQuery
  alias Logflare.Sources
  alias Logflare.Sources.Source.Data

  setup do
    insert(:plan)

    user =
      insert(:user,
        bigquery_project_id: "custom-project",
        bigquery_dataset_id: "custom_dataset"
      )

    token = TestUtils.gen_uuid_atom()
    assert Sources.Cache.get_by(token: token) == nil
    source = insert(:source, token: token, user: user)
    table_name = token |> Atom.to_string() |> String.replace("-", "_")

    table = %Table{
      numRows: "123",
      streamingBuffer: %Streamingbuffer{estimatedRows: "7"}
    }

    expect(Tables, :bigquery_tables_get, fn
      _conn, "custom-project", "custom_dataset", ^table_name -> {:ok, table}
    end)

    {:ok, source: source, table: table}
  end

  describe "get_table/1" do
    test "uses the source struct when its token cache entry holds nil", %{
      source: source,
      table: table
    } do
      assert {:ok, ^table} = BigQuery.get_table(source)
      assert Sources.Cache.get_by(token: source.token) == nil
    end

    test "continues to accept an atom token for a cached source", %{source: source, table: table} do
      assert %{id: id} = Sources.Cache.get_by_token_or_primary(source.token)
      assert id == source.id

      assert {:ok, ^table} = BigQuery.get_table(source.token)
    end
  end

  test "get_log_count/2 sums table and streaming-buffer rows without rereading the token cache",
       %{
         source: source
       } do
    assert Data.get_log_count(source, "custom-project") == 130
    assert Sources.Cache.get_by(token: source.token) == nil
  end
end
