defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  doctest EventUtils

  describe "log_event_to_df_struct/1" do
    test "preserves the timestamp type required by BigQuery Storage API encoding" do
      source = %Source{id: 1, name: "storage-contract-test", validate_schema: false}
      timestamp = "2024-02-29T12:34:56.123456Z"
      {:ok, expected, _offset} = DateTime.from_iso8601(timestamp)

      log_event =
        LogEvent.make(
          %{
            "id" => "00000000-0000-0000-0000-000000000000",
            "event_message" => "storage contract",
            "timestamp" => timestamp
          },
          %{source: source}
        )

      assert log_event.body["timestamp"] == DateTime.to_unix(expected, :microsecond)

      row = EventUtils.log_event_to_df_struct(log_event)
      assert row["timestamp"] == expected

      dataframe = Explorer.DataFrame.new([row])

      assert Explorer.DataFrame.dtypes(dataframe)["timestamp"] ==
               {:datetime, :microsecond, "Etc/UTC"}

      assert {:ok, _schema} = Explorer.DataFrame.dump_ipc_schema(dataframe)
      assert {:ok, [_batch | _]} = Explorer.DataFrame.dump_ipc_record_batch(dataframe)
    end
  end

  describe "prepare_for_ingest/1" do
    test "wraps event in list and nested maps in lists" do
      event = %{"message" => "hello", "metadata" => %{"user_id" => "123"}}

      result = EventUtils.prepare_for_ingest(event)
      expected = [%{"message" => "hello", "metadata" => [%{"user_id" => "123"}]}]

      assert result == expected
    end

    test "handles lists of maps unchanged" do
      event = %{"tags" => [%{"key" => "env", "value" => "prod"}]}

      result = EventUtils.prepare_for_ingest(event)

      assert result == [event]
    end

    test "handles nested list-of-lists (e.g. a serialized stacktrace)" do
      event = %{
        "stacktrace" => [
          ["Elixir.ProjectThree.TmAmTicket", "ingest", 2],
          ["Elixir.ProjectThree.TmHost", "dispatch", 1]
        ]
      }

      assert EventUtils.prepare_for_ingest(event) == [event]
    end

    test "handles a list whose head is a map but tail contains lists and scalars" do
      event = %{
        "mixed" => [
          %{"a" => %{"b" => 1}},
          ["nested", "list"],
          "scalar"
        ]
      }

      result = EventUtils.prepare_for_ingest(event)

      expected = [
        %{
          "mixed" => [
            %{"a" => [%{"b" => 1}]},
            ["nested", "list"],
            "scalar"
          ]
        }
      ]

      assert result == expected
    end
  end
end
