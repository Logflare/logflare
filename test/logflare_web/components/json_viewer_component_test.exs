defmodule LogflareWeb.JSONViewerComponentTest do
  use LogflareWeb.ConnCase, async: true
  use ExUnitProperties

  import Phoenix.LiveViewTest
  import LogflareWeb.JSONViewerComponent
  import StreamData

  # Custom generators for property tests

  defp json_value do
    one_of([
      string(:printable, max_length: 50),
      integer(-1000..1000),
      float(min: -1000.0, max: 1000.0),
      boolean(),
      constant(nil)
    ])
  end

  defp json_like_map_or_list do
    json_like_map_or_list(10)
  end

  defp json_like_map_or_list(depth) do
    one_of([
      map_of(string(:alphanumeric, min_length: 1, max_length: 10), json_like_term(depth - 1),
        max_length: 3
      ),
      list_of(json_like_term(depth - 1), max_length: 3)
    ])
  end

  defp json_like_term(0) do
    json_value()
  end

  defp json_like_term(depth) do
    frequency([
      {3, json_value()},
      {1,
       map_of(string(:alphanumeric, min_length: 1, max_length: 10), json_like_term(depth - 1),
         max_length: 3
       )},
      {1, list_of(json_like_term(depth - 1), max_length: 3)}
    ])
  end

  defp extract_keys_and_values(map) when is_map(map) do
    Enum.flat_map(map, fn {k, v} ->
      [{:key, k} | extract_keys_and_values(v)]
    end)
  end

  defp extract_keys_and_values(list) when is_list(list) do
    Enum.flat_map(list, &extract_keys_and_values/1)
  end

  defp extract_keys_and_values(value) do
    [{:value, value}]
  end

  defp format_value_for_html(value) when is_binary(value), do: ~s("#{value}")
  defp format_value_for_html(value) when is_integer(value), do: Integer.to_string(value)
  defp format_value_for_html(value) when is_float(value), do: Float.to_string(value)
  defp format_value_for_html(true), do: "true"
  defp format_value_for_html(false), do: "false"
  defp format_value_for_html(nil), do: "null"

  defp extract_text_content(html) do
    html
    |> Floki.parse_document!()
    |> Floki.text(sep: " ")
  end

  describe "json_viewer/1" do
    test "renders simple map with string values" do
      data = %{"user" => %{"name" => "John", "age" => "30"}}
      html = render_component(&json_viewer/1, data: data)

      doc = Floki.parse_document!(html)

      assert Floki.find(doc, "#user") != []
      assert Floki.find(doc, "#user--age") != []
      assert Floki.find(doc, "#user--name") != []

      string_values =
        doc
        |> Floki.find("span.tw-text-json-tree-string")
        |> Enum.map(&Floki.text/1)
        |> Enum.map(&String.trim/1)

      assert Enum.sort(string_values) == Enum.sort(["\"30\"", "\"John\""])
    end

    test "renders simple list with values" do
      data = %{"items" => ["apple", "banana", "cherry"]}
      html = render_component(&json_viewer/1, data: data)

      assert html =~ "Array"
      assert html =~ "items"
      assert html =~ "0:"
      assert html =~ "1:"
      assert html =~ "2:"
      assert html =~ "apple"
      assert html =~ "banana"
      assert html =~ "cherry"
    end

    test "renders deeply nested structure (3+ levels)" do
      data = %{
        "level1" => %{
          "level2" => %{
            "level3" => %{
              "value" => "deep"
            }
          }
        }
      }

      html = render_component(&json_viewer/1, data: data)

      assert html =~ "level1"
      assert html =~ "level2"
      assert html =~ "level3"
      assert html =~ "value"
      assert html =~ "deep"
    end

    test "applies class for value types" do
      data = %{
        "nested" => %{
          "str" => "text",
          "num" => 123,
          "bool" => true,
          "null" => nil
        }
      }

      html = render_component(&json_viewer/1, data: data)

      assert html =~ ~s(tw-text-json-tree-key)
      assert html =~ ~s(tw-text-json-tree-string)
      assert html =~ ~s(tw-text-json-tree-number)
      assert html =~ ~s(tw-text-json-tree-boolean)
      assert html =~ ~s(tw-text-json-tree-null)
      assert html =~ ~s(tw-text-json-tree-label)
    end

    test "renders URL as link" do
      data = %{"website" => "https://example.com"}
      html = render_component(&json_viewer/1, data: data)

      assert html =~ ~s(href="https://example.com")
      assert html =~ ~s(target="_blank")
    end
  end

  describe "property tests" do
    property "renders any JSON map or list without crashing" do
      ExUnitProperties.check all(data <- json_like_map_or_list()) do
        html = render_component(&json_viewer/1, data: data)
        assert is_binary(html)
      end
    end

    property "all keys and values at any depth appear in rendered output" do
      ExUnitProperties.check all(data <- json_like_map_or_list()) do
        html = render_component(&json_viewer/1, data: data)
        text_content = extract_text_content(html)
        keys_and_values = extract_keys_and_values(data)

        for {type, item} <- keys_and_values do
          case type do
            :key ->
              assert text_content =~ item,
                     "Expected key #{inspect(item)} in rendered text"

            :value ->
              expected = format_value_for_html(item)

              assert text_content =~ expected,
                     "Expected value #{inspect(item)} (formatted as #{inspect(expected)}) in rendered text"
          end
        end
      end
    end
  end
end
