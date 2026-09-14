defmodule LogflareWeb.SearchLive.EventPaginationTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.SearchLive.EventPagination

  describe "buttons/2" do
    test "a page request in flight puts only its own button in the loading state" do
      previous_cursor = %{id: "previous", timestamp: 1}
      next_cursor = %{id: "next", timestamp: 2}
      cursors = %{previous: previous_cursor, next: next_cursor}

      opts = [cursors: cursors, tailing?: false, loading?: false]

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
        |> EventPagination.buttons(cursors: cursors, tailing?: false, loading?: false)

      assert %{previous: %{state: :hidden}, next: %{state: :hidden}} = buttons
    end

    test "shows a button whenever it has a cursor and the view is not tailing" do
      previous_cursor = %{id: "previous", timestamp: 1}
      next_cursor = %{id: "next", timestamp: 2}
      pagination = EventPagination.new()

      buttons =
        EventPagination.buttons(pagination,
          cursors: %{previous: previous_cursor, next: next_cursor},
          tailing?: false,
          loading?: false
        )

      assert %{
               previous: %{state: :ready, cursor: ^previous_cursor},
               next: %{state: :ready, cursor: ^next_cursor}
             } = buttons
    end

    test "hides a button with no cursor, and both while tailing" do
      cursor = %{id: "cursor", timestamp: 1}
      pagination = EventPagination.new()

      assert %{previous: %{state: :hidden}, next: %{state: :ready}} =
               EventPagination.buttons(pagination,
                 cursors: %{previous: nil, next: cursor},
                 tailing?: false,
                 loading?: false
               )

      assert %{previous: %{state: :hidden}, next: %{state: :hidden}} =
               EventPagination.buttons(pagination,
                 cursors: %{previous: cursor, next: cursor},
                 tailing?: true,
                 loading?: false
               )
    end

    test "disables both buttons while a search is running" do
      cursor = %{id: "cursor", timestamp: 1}

      assert %{previous: %{state: :disabled}, next: %{state: :disabled}} =
               EventPagination.buttons(EventPagination.new(),
                 cursors: %{previous: cursor, next: cursor},
                 tailing?: false,
                 loading?: true
               )
    end
  end
end
