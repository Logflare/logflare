defmodule Logflare.Source.BigQuery.SchemaTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory
  use Placebo

  setup do
    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, s1} = Sources.create_source(params_for(:source), u1)

    {:ok, sources: [s1]}
  end

  describe "Schema GenServer" do
    test "start_link/1", %{sources: [s1 | _]} do
      sid = s1.token
      rls = %RLS{source_id: sid, plan: %{limit_source_fields_limit: 500}}

      {:ok, _pid} = Schema.start_link(rls)

      schema = Schema.get_state(sid)

      assert %{schema | next_update: :placeholder} == %{
               bigquery_project_id: nil,
               bigquery_dataset_id: nil,
               source_token: sid,
               next_update: :placeholder,
               field_count_limit: 500
             }
    end
  end
end
