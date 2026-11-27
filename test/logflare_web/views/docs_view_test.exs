defmodule LogflareWeb.DocsViewTest do
  use LogflareWeb.ConnCase, async: true
  import LogflareWeb.DocsView
  doctest LogflareWeb.DocsView

  describe "render_docs/2" do
    test "renders markdown tables" do
      assert render_docs("concepts/lql") =~ "<table>"
    end

    test "returns an empty string when the anchor does not exist" do
      assert render_docs("concepts/lql#no-such-section") == ""
    end

    test "an anchored section stops at the next heading of the same level" do
      html = render_docs("concepts/lql#event-message-filtering")

      assert html =~ "Event Message Filtering"
      refute html =~ "Metadata Filtering"
    end

    test "an anchored section keeps its header unless hide_header is set" do
      assert render_docs("concepts/lql#event-message-filtering") =~ "<h4>"
      refute render_docs("concepts/lql#event-message-filtering", hide_header: true) =~ "<h4>"
    end

    test "front matter is not rendered" do
      refute render_docs("concepts/lql") =~ "sidebar_position"
    end

    test "hide_header removes the page title" do
      assert render_docs("concepts/lql") =~ "<h1>Logflare Query Language (LQL)</h1>"
      refute render_docs("concepts/lql", hide_header: true) =~ "<h1>"
    end

    test "relative image urls point at the served docs assets" do
      html = render_docs("concepts/lql")

      assert html =~ ~s(src="/docs/assets/concepts/lql/lql-reference.png")
      assert html =~ ~s(src="/docs/assets/concepts/lql/ai-assisted-search.png")
      refute html =~ ~s(src="./)
    end
  end
end
