defmodule Logflare.Logs.SearchTest do
  @moduledoc false
  alias Logflare.Sources
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Pipeline
  use Logflare.DataCase
  import Logflare.DummyFactory

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "Search" do
    test "utc_today", %{sources: [source | _], users: [user | _]} do
      les = build_list(3, :log_event, source: source)
      bq_rows = Enum.map(les, &Pipeline.le_to_bq_row/1)
      project_id = GenUtils.get_project_id(source.token)

      assert {:ok, _} = BigQuery.create_dataset("#{user.id}", project_id)
      assert {:ok, _} = BigQuery.create_table(source.token)
      assert {:ok, _} = BigQuery.stream_batch!(source.token, bq_rows)
      {:ok, %{result: search_results}} = Logflare.Logs.Search.utc_today(%{source: source})
      assert length(search_results) == 3
    end
  end
end
