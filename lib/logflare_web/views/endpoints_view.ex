defmodule LogflareWeb.EndpointsView do
  use LogflareWeb, :view
  alias Logflare.SingleTenant
  def render("query.json", %{result: data}) do
    %{result: data}
  end

  def render("query.json", %{error: error}) do
    %{error: error}
  end
end
