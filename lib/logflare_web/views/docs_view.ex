defmodule LogflareWeb.DocsView do
  use LogflareWeb, :live_view_with_templates

  alias Logflare.MixTasks.Docs
  alias MDEx.Document
  alias MDEx.FrontMatter
  alias MDEx.Heading
  alias MDEx.HtmlInline
  alias MDEx.Image
  alias MDEx.Text

  Docs.run(nil)

  @assets_path "/docs/assets"

  @markdown_options [
    extension: [table: true, autolink: true, front_matter_delimiter: "---"],
    render: [unsafe: true]
  ]

  @doc """
  This is used to render docs markdown pages within the app.

    iex>  render_docs("concepts") =~ "Logflare is a log ingestion and querying engine"
    true

  We do not need to specify the trailing `index` filename
    iex> render_docs("concepts/lql") =~ "Logflare Query Language"
    true

  We can filter down to a specific html anchor
    iex> render_docs("concepts/lql#event-message-filtering") =~ "Event Message Filtering"
    true
    iex> render_docs("concepts/lql#event-message-filtering") =~ "Live Search"
    false

  Specific html entities are also replaced

    iex> render_docs("concepts") =~ ~S(;#124;)
    false

  Replace line breaks with newlines

    iex> escaped = "<br/>" |> Phoenix.HTML.html_escape() |> Phoenix.HTML.safe_to_string()
    iex> render_docs("concepts/lql") =~ escaped
    false

  Inline html in tables are also fixed and replaced

    iex> escaped = "<code>" |> Phoenix.HTML.html_escape() |> Phoenix.HTML.safe_to_string()
    iex> render_docs("concepts/lql") =~ escaped
    false

  We can also hide the top level header

    iex> render_docs("concepts/lql#event-message-filtering", hide_header: true) |> String.slice(0, 29)
    "<p>Any string not matching a "


  """
  def render_docs(rel_path, opts \\ []) do
    opts = Enum.into(opts, %{hide_header: false})

    [rel_path, anchor] =
      case String.split(rel_path, "#") do
        [path, anchor] -> [path, anchor]
        path -> [path, nil]
      end

    docs_path = Path.join(:code.priv_dir(:logflare), "docs")
    file_path = Path.join(docs_path, rel_path)

    file =
      (Path.wildcard(file_path <> "/index.{md,mdx}") ++ Path.wildcard(file_path <> ".{md,mdx}"))
      |> hd()

    doc_dir = file |> Path.dirname() |> Path.relative_to(docs_path)

    File.read!(file)
    |> MDEx.parse_document!(@markdown_options)
    |> drop_front_matter()
    |> rewrite_image_urls(doc_dir)
    |> filter_to_anchor(anchor)
    |> replace_line_breaks()
    |> maybe_hide_header(opts.hide_header)
    |> MDEx.to_html!(@markdown_options)
  end

  defp drop_front_matter(%Document{nodes: [%FrontMatter{} | rest]} = document),
    do: %{document | nodes: rest}

  defp drop_front_matter(document), do: document

  defp rewrite_image_urls(%Document{} = document, doc_dir) do
    MDEx.traverse_and_update(document, fn
      %Image{url: url} = image -> %{image | url: asset_url(url, doc_dir)}
      node -> node
    end)
  end

  defp asset_url(url, doc_dir) do
    case URI.parse(url) do
      %URI{scheme: nil, host: nil, path: "/" <> _} ->
        url

      %URI{scheme: nil, host: nil, path: path} = uri when is_binary(path) and path != "" ->
        asset_path = Path.expand(Path.join([@assets_path, doc_dir, path]))
        URI.to_string(%{uri | path: asset_path})

      _uri ->
        url
    end
  end

  defp filter_to_anchor(document, nil), do: document

  defp filter_to_anchor(%Document{nodes: nodes} = document, anchor) when is_binary(anchor) do
    anchor_parts = String.split(anchor, "-")

    section =
      nodes
      |> Enum.drop_while(&(not anchor_heading?(&1, anchor_parts)))
      |> take_section()

    %{document | nodes: section}
  end

  defp anchor_heading?(%Heading{level: level, nodes: [%Text{literal: text}]}, anchor_parts)
       when level in 1..4 do
    heading_parts(text) == anchor_parts
  end

  defp anchor_heading?(_node, _anchor_parts), do: false

  defp heading_parts(text) do
    text
    |> String.downcase()
    |> String.replace("(", " ")
    |> String.replace(")", " ")
    |> String.split()
  end

  defp take_section([%Heading{level: level} = heading | rest]) do
    [heading | Enum.take_while(rest, &(not section_end?(&1, level)))]
  end

  defp take_section([]), do: []

  defp section_end?(%Heading{level: level}, section_level), do: level <= section_level
  defp section_end?(_node, _section_level), do: false

  defp replace_line_breaks(%Document{} = document) do
    MDEx.traverse_and_update(document, fn
      %HtmlInline{literal: "<br/>"} -> %Text{literal: "\n"}
      node -> node
    end)
  end

  defp maybe_hide_header(%Document{nodes: [%Heading{} | rest]} = document, true),
    do: %{document | nodes: rest}

  defp maybe_hide_header(document, _hide_header), do: document
end
