defmodule LogflareWeb.SearchLive.EventPaginationTest do
  use ExUnit.Case, async: true

  alias Logflare.Logs.EventPage
  alias LogflareWeb.SearchLive.EventPagination

  describe "buttons/2" do
    test "a page request in flight puts only its own button in the loading state" do
      previous_cursor = %{id: "previous", timestamp: 1}
      next_cursor = %{id: "next", timestamp: 2}
      cursors = %{previous: previous_cursor, next: next_cursor}

      opts = [cursors: cursors, tailing?: false, loading?: false, next_available?: true]

      loading_previous =
        EventPagination.new()
        |> EventPagination.mark_loading(:previous)
        |> EventPagination.buttons(opts)

      assert %{previous: %{state: :loading}, next: %{state: :ready}} = loading_previous

      loading_next =
        EventPagination.new()
        |> EventPagination.mark_loading(:next)
        |> EventPagination.buttons(opts)

      assert %{previous: %{state: :ready}, next: %{state: :loading}} = loading_next

      cleared =
        EventPagination.new()
        |> EventPagination.mark_loading(:next)
        |> EventPagination.clear_loading()
        |> EventPagination.buttons(opts)

      assert %{previous: %{state: :ready}, next: %{state: :ready}} = cleared
    end

    test "a hidden button stays hidden while a page request is in flight" do
      cursors = %{previous: nil, next: nil}

      buttons =
        EventPagination.new()
        |> EventPagination.mark_loading(:previous)
        |> EventPagination.buttons(
          cursors: cursors,
          tailing?: false,
          loading?: false,
          next_available?: false
        )

      assert %{previous: %{state: :hidden}, next: %{state: :hidden}} = buttons
    end

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
