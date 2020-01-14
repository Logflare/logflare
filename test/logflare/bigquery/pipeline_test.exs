defmodule Logflare.Google.BigQuery.PipelineTest do
  @moduledoc false
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.{Sources, LogEvent}
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  use Logflare.DataCase

  setup do
    u = insert(:user, email: System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM"))
    s = insert(:source, user_id: u.id)
    s = Sources.get_by(id: s.id)
    {:ok, sources: [s], users: [u]}
  end

  describe "Pipeline" do
    test "le_to_bq_row/1", %{sources: [source | _], users: [_user | _]} do
      datetime = DateTime.utc_now()

      le =
        LogEvent.make(
          %{
            "message" => "valid",
            "timestamp" => datetime |> DateTime.to_unix(:microsecond)
          },
          %{source: source}
        )

      assert Pipeline.le_to_bq_row(le) ==
               %TableDataInsertAllRequestRows{
                 insertId: le.id,
                 json: %{
                   "event_message" => "valid",
                   "timestamp" => datetime,
                   "id" => le.id
                 }
               }

      le =
        LogEvent.make(
          %{
            "message" => "valid",
            "metadata" => %{},
            "timestamp" => datetime |> DateTime.to_unix(:microsecond)
          },
          %{source: source}
        )

      assert Pipeline.le_to_bq_row(le) ==
               %TableDataInsertAllRequestRows{
                 insertId: le.id,
                 json: %{
                   "event_message" => "valid",
                   "timestamp" => datetime
                 }
               }
    end
  end
end
