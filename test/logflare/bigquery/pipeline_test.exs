defmodule Logflare.BigQuery.PipelineTest do
  @moduledoc false
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.{LogEvent}
  alias GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows
  use Logflare.DataCase

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    {:ok, source: source}
  end

  test "le_to_bq_row/1 generates TableDataInsertAllRequestRows struct correctly", %{
    source: source
  } do
    datetime = DateTime.utc_now()

    le =
      LogEvent.make(
        %{
          "message" => "valid",
          "metadata" => %{"a" => "nested"},
          "timestamp" => datetime |> DateTime.to_unix(:microsecond)
        },
        %{source: source}
      )

    id = le.id

    assert %TableDataInsertAllRequestRows{
             insertId: ^id,
             json: %{
               event_message: "valid",
               timestamp: ^datetime,
               metadata: [%{"a" => "nested"}],
               id: ^id,
               # not too sure about these fields
               level: nil,
               project: nil
             }
           } = Pipeline.le_to_bq_row(le)
  end
end
