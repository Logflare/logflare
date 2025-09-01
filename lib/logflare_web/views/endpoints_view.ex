defmodule LogflareWeb.EndpointsView do
  use LogflareWeb, :view

  def render("query.json", %{result: data}) when not is_nil(data) do
    %{result: data}
  end

  def render("query.json", %{error: errors}) do
    %{error: errors}
  end
end
