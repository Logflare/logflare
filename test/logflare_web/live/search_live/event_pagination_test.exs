defmodule LogflareWeb.SearchLive.EventPaginationTest do
  use ExUnit.Case, async: true

  alias LogflareWeb.SearchLive.EventPagination

  @cursors %{previous: %{id: "previous", timestamp: 1}, next: %{id: "next", timestamp: 2}}
  @ready_opts [cursors: @cursors, tailing?: false, loading?: false]

  describe "buttons/2" do
    test "a page request in flight spins its own button and disables the other" do
      loading_previous =
        EventPagination.new()
        |> EventPagination.mark_loading(:previous, 10)
        |> EventPagination.buttons(@ready_opts)

      assert %{previous: %{state: :loading}, next: %{state: :disabled}} = loading_previous

      loading_next =
        EventPagination.new()
        |> EventPagination.mark_loading(:next, 10)
        |> EventPagination.buttons(@ready_opts)

      assert %{previous: %{state: :disabled}, next: %{state: :loading}} = loading_next

      cleared =
        EventPagination.new()
        |> EventPagination.mark_loading(:next, 10)
        |> EventPagination.clear_loading()
        |> EventPagination.buttons(@ready_opts)

      assert %{previous: %{state: :ready}, next: %{state: :ready}} = cleared
    end

    test "a hidden button stays hidden while a page request is in flight" do
      cursors = %{previous: nil, next: nil}

      buttons =
        EventPagination.new()
        |> EventPagination.mark_loading(:previous, 10)
        |> EventPagination.buttons(cursors: cursors, tailing?: false, loading?: false)

      assert %{previous: %{state: :hidden}, next: %{state: :hidden}} = buttons
    end

    test "shows a button whenever it has a cursor and the view is not tailing" do
      %{previous: previous_cursor, next: next_cursor} = @cursors

      assert %{
               previous: %{state: :ready, cursor: ^previous_cursor},
               next: %{state: :ready, cursor: ^next_cursor}
             } = EventPagination.buttons(EventPagination.new(), @ready_opts)
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
      assert %{previous: %{state: :disabled}, next: %{state: :disabled}} =
               EventPagination.buttons(EventPagination.new(),
                 cursors: @cursors,
                 tailing?: false,
                 loading?: true
               )
    end

    test "labels both buttons with the stored window" do
      assert %{previous: %{label: "Load more"}, next: %{label: "Load more"}} =
               EventPagination.buttons(EventPagination.new(), @ready_opts)

      assert %{
               previous: %{label: "Load more (-10 minutes)"},
               next: %{label: "Load more (+10 minutes)"}
             } =
               EventPagination.new()
               |> EventPagination.put_window(600)
               |> EventPagination.buttons(@ready_opts)
    end
  end

  describe "loading?/2" do
    test "matches only the intent in flight" do
      pagination = EventPagination.mark_loading(EventPagination.new(), :previous, 10)

      assert EventPagination.loading?(pagination, :previous)
      refute EventPagination.loading?(pagination, :next)
      refute EventPagination.loading?(EventPagination.new(), :previous)
      refute EventPagination.loading?(EventPagination.clear_loading(pagination), :previous)
    end
  end
end
