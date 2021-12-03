defmodule LogflareWeb.EndpointView do
    import LogflareWeb.Helpers.Forms
    use LogflareWeb, :view

    def render("query.json", %{result: data}) do
        %{result: data}
    end

    def render("query.json", %{error: error}) do
        %{error: error}
    end

    def render("sandbox_schema.json", %{schema: data}) do
        %{schema: data}
    end

    def render("sandbox_schema.json", %{error: error}) do
        %{error: error}
    end

  end
