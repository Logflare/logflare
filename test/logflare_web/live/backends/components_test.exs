defmodule LogflareWeb.Backends.ComponentsTest do
  use ExUnit.Case

  import Phoenix.LiveViewTest

  alias LogflareWeb.Backends.Components
  alias Phoenix.LiveView.AsyncResult

  describe "status_indicator/1" do
    test "renders a spinner while the async result is loading" do
      html = render_component(&Components.status_indicator/1, %{status: AsyncResult.loading()})

      assert html =~ "fa-spinner"
      assert html =~ "tw-animate-spin"
      refute html =~ "fa-times"
      refute html =~ "fa-check"
    end

    test "renders an error icon and reason when the async result failed" do
      result = AsyncResult.failed(AsyncResult.loading(), {:error, "boom"})
      html = render_component(&Components.status_indicator/1, %{status: result})

      assert html =~ "fa-times"
      assert html =~ "tw-text-red-500"
      assert html =~ "boom"
      refute html =~ "fa-spinner"
      refute html =~ "fa-check"
    end

    test "renders a generic 'Internal error' message on non-error failure" do
      result = AsyncResult.failed(AsyncResult.loading(), {:exit, :boom})
      html = render_component(&Components.status_indicator/1, %{status: result})

      assert html =~ "fa-times"
      assert html =~ "Internal error"
      refute html =~ "fa-spinner"
      refute html =~ "fa-check"
    end

    test "renders a check icon when the async result is ok" do
      result = AsyncResult.ok(AsyncResult.loading(), :connected)
      html = render_component(&Components.status_indicator/1, %{status: result})

      assert html =~ "fa-check"
      assert html =~ "tw-text-green-500"
      refute html =~ "fa-spinner"
      refute html =~ "fa-times"
    end
  end
end
