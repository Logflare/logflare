defmodule LogflareWeb.PaginationComponentsTest do
  use ExUnit.Case

  import Phoenix.LiveViewTest

  alias LogflareWeb.PaginationComponents

  describe "pagination_links/1" do
    test "renders pagination with basic page structure" do
      page = %Scrivener.Page{
        page_number: 1,
        total_pages: 5,
        total_entries: 50,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts"
        })

      assert html =~ "<nav>"
      assert html =~ ~s(<ul class="pagination">)
      assert html =~ ~s(<li class="page-item active">)
    end

    test "shows current page as active" do
      page = %Scrivener.Page{
        page_number: 3,
        total_pages: 5,
        total_entries: 50,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts"
        })

      assert html =~ ~s(<li class="page-item active">)
      assert html =~ ">3<"
    end

    test "includes additional params in URLs" do
      page = %Scrivener.Page{
        page_number: 1,
        total_pages: 5,
        total_entries: 50,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts",
          params: [sort_by: "recent"]
        })

      assert html =~ "sort_by=recent"
    end

    test "omits page param from first page URL" do
      page = %Scrivener.Page{
        page_number: 2,
        total_pages: 5,
        total_entries: 50,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts"
        })

      # First page link should not have ?page=1
      assert html =~ ~s(href="/admin/accounts")
    end

    test "includes page param for non-first pages" do
      page = %Scrivener.Page{
        page_number: 1,
        total_pages: 5,
        total_entries: 50,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts"
        })

      # Second page link should have ?page=2
      assert html =~ "page=2"
    end

    test "can hide first and last page links" do
      page = %Scrivener.Page{
        page_number: 10,
        total_pages: 20,
        total_entries: 200,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts",
          distance: 2,
          first: false,
          last: false
        })

      refute html =~ ">1<"
      refute html =~ ">20<"
    end

    test "sets proper rel attributes" do
      page = %Scrivener.Page{
        page_number: 5,
        total_pages: 10,
        total_entries: 100,
        page_size: 10,
        entries: []
      }

      html =
        render_component(&PaginationComponents.pagination_links/1, %{
          page: page,
          path: "/admin/accounts"
        })

      assert html =~ ~s(href="/admin/accounts?page=4" rel="prev")
      assert html =~ ~s(href="/admin/accounts?page=6" rel="next")
      assert html =~ ~s(rel="canonical")
    end
  end
end
