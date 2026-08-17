defmodule LogflareWeb.SearchLive.EventPaginationTest do
  use ExUnit.Case, async: true

  alias Logflare.Logs.EventPage
  alias LogflareWeb.SearchLive.EventPagination

  describe "buttons/2" do
    test "determines visibility from cursor and exhaustion state" do
      previous_cursor = %{id: "previous", timestamp: 1}
      next_cursor = %{id: "next", timestamp: 2}

      pagination =
        EventPagination.new()
        |> EventPagination.complete_initial(%EventPage{
          rows: [],
          request: %{intent: :initial, cursor: nil},
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
            request: %{intent: :next, cursor: next_cursor},
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
  end
end
