defmodule LogtailWeb.LogView do
  use LogtailWeb, :view

  def render("index.json", %{message: message}) do
    %{message: message}
  end

end
