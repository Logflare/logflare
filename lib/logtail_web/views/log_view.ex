defmodule LogtailWeb.LogView do
  use LogtailWeb, :view

  def render("index.json", %{message: message}) do
    IO.inspect message
    %{message: message}
  end

end
