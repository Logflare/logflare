defmodule LogflareWeb.DocsView do
  use LogflareWeb, :live_view_with_templates

  Mix.Tasks.Docs.run(nil)

  @doc """
  This is used to render docs markdown pages within the app.

    iex>  render_docs("intro") =~ "Logflare is a log ingestion and querying engine"
    true

  We do not need to specify the trailing `index` filename
    iex> render_docs("querying") =~ "Logflare Query Language"
    true

  We can filter down to a specific html anchor
    iex> render_docs("querying#logflare-query-language-lql") =~ "Logflare Query Language (LQL)"
    true
    iex> render_docs("querying#logflare-query-language-lql") =~ "Event Message Filtering"
    true
    iex> render_docs("querying#logflare-query-language-lql") =~ "Live Search"
    false

  Specific html entities are also replaced

    iex> render_docs("querying") =~ ~S(;#124;)
    false

  Replace line breaks with newlines

    iex> escaped = "<br/>" |> Phoenix.HTML.html_escape() |> Phoenix.HTML.safe_to_string()
    iex> render_docs("querying") =~ escaped
    false

  Inline html in tables are also fixed and replaced
    iex> escaped = "<code>" |> Phoenix.HTML.html_escape() |> Phoenix.HTML.safe_to_string()
    iex> render_docs("querying") =~ escaped
    false



  We can also hide the top level header

    iex> render_docs("querying#logflare-query-language-lql", hide_header: true) |> String.slice(0, 31)
    "<p>\\nThe Logflare Query Language"


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

    File.read!(file)
    |> Earmark.as_ast!()
    |> filter_to_anchor(anchor)
    |> replace_html_entities()
    |> replace_line_breaks()
    |> replace_table_inline_code_tag()
    |> maybe_hide_header(opts.hide_header)
    |> Earmark.Transform.transform()
  end

  # filters down to a particular section
  defp filter_to_anchor(ast, nil), do: ast

  defp filter_to_anchor(ast, anchor) when is_binary(anchor) do
    {ast, _value} =
      ast
      |> Earmark.Transform.map_ast_with(
        false,
        fn
          # not matched yet, check if header node is matching
          {tag, _attrs, [child], _meta} = node, false when tag in ["h1", "h2", "h3", "h4"] ->
            header_parts =
              child
              |> String.downcase()
              |> String.replace("(", " ")
              |> String.replace(")", " ")
              |> String.split()

            if header_parts == String.split(anchor, "-") do
              {node, {true, tag}}
            else
              {{"drop", [], [], %{}}, false}
            end

          # node not matched yet, continue
          {_tag, _attrs, _child, _meta}, false ->
            {{"drop", [], [], %{}}, false}

          # matched, have not met stop cond,  keep node
          {tag, _attrs, _children, _meta} = node, {true, _} = acc
          when tag not in ["h1", "h2", "h3", "h4"] ->
            {node, acc}

          # node matched, is header node, check if stop cond
          {"h" <> num, _attrs, _children, _meta} = node, {true, "h" <> start_num} = acc ->
            {num, _} = Integer.parse(num)
            {start_num, _} = Integer.parse(start_num)

            if start_num < num do
              # lower level header, keep
              {node, acc}
            else
              {{"drop", [], [], %{}}, :halt}
            end

          {_tag, _attrs, _children, _meta} = node, :halt ->
            {{"drop", [], [], %{}}, :halt}

          node, acc ->
            {node, acc}
        end
      )

    ast
    |> Enum.reject(fn
      {"drop", _attrs, _children, _meta} -> true
      _ -> false
    end)
  end

  defp replace_html_entities(ast) do
    ast
    |> Earmark.Transform.map_ast(fn
      text when is_binary(text) -> String.replace(text, "&#124;", "|")
      other -> other
    end)
  end

  defp replace_line_breaks(ast) do
    ast
    |> Earmark.Transform.map_ast(fn
      text when is_binary(text) -> String.replace(text, "<br/>", "\n")
      other -> other
    end)
  end

  defp replace_table_inline_code_tag(ast) do
    ast
    |> Earmark.Transform.map_ast(fn
      "<code>" <> text when is_binary(text) ->
        # TODO: naive, use regex
        new_text =
          text
          |> String.replace("</code>", "")

        {"code", [], [new_text], %{}}

      other ->
        other
    end)
  end

  defp maybe_hide_header([{"h" <> _, _attrs, _children, _meta} | rest], true), do: rest
  defp maybe_hide_header(ast, _), do: ast
end
