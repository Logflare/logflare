defmodule LogflareWeb.SearchLive.EventPaginationTest do
  use ExUnit.Case, async: true

  alias Logflare.Logs.EventPage
  alias LogflareWeb.SearchLive.EventPagination

  describe "buttons/2" do
    test "derives visibility from cursor and exhaustion state" do
      previous_cursor = %{id: "previous", timestamp: 1}
      next_cursor = %{id: "next", timestamp: 2}

      pagination =
        EventPagination.new()
        |> EventPagination.complete_initial(%EventPage{
          rows: [],
          request: %{intent: :initial, boundary: nil, cursor: nil},
          cursor: previous_cursor,
          next_cursor: next_cursor,
          has_more?: true
        })

      buttons =
        EventPagination.buttons(pagination,
          cursors: %{previous: previous_cursor, next: next_cursor},
          tailing?: false,
          loading?: false,
          next_available?: true
        )

      assert %{
               previous: %{state: :ready, cursor: ^previous_cursor},
               next: %{state: :ready, cursor: ^next_cursor}
             } = buttons

      hidden_next_buttons =
        EventPagination.buttons(pagination,
          cursors: %{previous: previous_cursor, next: next_cursor},
          tailing?: false,
          loading?: false,
          next_available?: false
        )

      assert %{next: %{state: :hidden, cursor: ^next_cursor}} = hidden_next_buttons

      pagination =
        EventPagination.complete_page(
          pagination,
          %EventPage{
            rows: [],
            request: %{intent: :next, boundary: nil, cursor: next_cursor},
            cursor: next_cursor,
            has_more?: false
          },
          :next
        )

      buttons =
        EventPagination.buttons(pagination,
          cursors: %{previous: previous_cursor, next: next_cursor},
          tailing?: false,
          loading?: false,
          next_available?: true
        )

      assert %{
               previous: %{state: :ready, cursor: ^previous_cursor},
               next: %{state: :hidden, cursor: ^next_cursor}
             } = buttons
    end

    test "a tail result makes the next control retryable" do
      pagination = %EventPagination{next_exhausted?: true}

      pagination =
        EventPagination.complete_tail(pagination, %EventPage{
          rows: [],
          request: %{intent: :initial, boundary: nil, cursor: nil},
          cursor: nil,
          next_cursor: %{id: "next", timestamp: 2},
          has_more?: false
        })

      refute pagination.next_exhausted?
    end
  end

  test "marks the expected range-extension patch" do
    pagination = EventPagination.new() |> EventPagination.mark_range_extension("t:today")

    assert pagination.range_extension == "t:today"
    assert EventPagination.clear_range_extension(pagination).range_extension == nil
  end
end
