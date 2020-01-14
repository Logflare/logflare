defmodule Logflare.Logs.SearchTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperations.SearchOperation, as: SO
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Pipeline
  use Logflare.DataCase
  import Logflare.Factory
  alias Logflare.Source.RecentLogsServer, as: RLS
  @test_dataset "test_dataset_01"
  @test_dataset_location "us-east4"

  setup do
    {:ok, _} = Logflare.Sources.Counters.start_link()

    user =
      insert(:user,
        email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"),
        bigquery_dataset_location: @test_dataset_location,
        bigquery_dataset_id: @test_dataset
      )

    s = insert(:source, user_id: user.id)
    source = Sources.get_by(id: s.id)

    {:ok, _} = RLS.start_link(%RLS{source_id: source.token})

    les =
      for x <- 1..5, y <- 100..101 do
        build(:log_event, message: "x#{x} y#{y}", source: source)
      end

    bq_rows = Enum.map(les, &Pipeline.le_to_bq_row/1)
    project_id = GenUtils.get_project_id(source.token)

    case BigQuery.create_dataset(
           "#{user.id}",
           @test_dataset,
           @test_dataset_location,
           project_id
         ) do
      {:ok, _} ->
        :noop

      {:error, tesla_env} ->
        if tesla_env.body =~ "Already Exists: Dataset" do
          :noop
        else
          throw(tesla_env)
        end
    end

    assert :ok = BigQuery.delete_table(source.token)
    assert {:ok, table} = BigQuery.create_table(source.token, @test_dataset, project_id, 300_000)
    assert {:ok, _} = BigQuery.stream_batch!(source.token, bq_rows)

    {:ok, sources: [source], users: [user]}
  end

  describe "Search" do
    test "search for source and regex", %{sources: [source | _], users: [_user | _]} do
      Process.sleep(1_000)
      search_op = %SO{source: source, querystring: ~S|x[123] \d\d1|}
      {:ok, %{rows: rows}} = Search.search(search_op)

      assert length(rows) == 3
    end
  end
end
