defmodule Logflare.Backends.SlackAdaptorTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Logflare.Backends.Adaptor.SlackAdaptor
  doctest Logflare.Backends.Adaptor.SlackAdaptor, except: [to_rich_text_preformatted: 1]

  test "to_rich_text_preformatted/1" do
    assert [%{text: "test:"}, %{text: " "}, %{text: "test"}] =
             to_rich_text_preformatted(%{"test" => "test"})

    assert [%{text: "test:"}, %{text: "\n"}, %{text: "test\ntest"}] =
             to_rich_text_preformatted(%{"test" => "test\ntest"})

    # multi-key
    assert [
             %{text: "a:"},
             %{text: " "},
             %{text: "a"},
             %{text: "\n"},
             %{text: "b:"},
             %{text: " "},
             %{text: "b"}
           ] = to_rich_text_preformatted(%{"a" => "a", "b" => "b"})

    # url handling
    assert [%{text: "test:"}, %{text: " "}, %{url: "http://" <> _}] =
             to_rich_text_preformatted(%{"test" => "http://test.com"})

    assert [%{text: "test:"}, %{text: " "}, %{url: "https://" <> _}] =
             to_rich_text_preformatted(%{"test" => "https://test.com"})
  end

  test "to_body/2" do
    assert %{
             blocks: [
               %{
                 type: "context",
                 elements: [
                   %{
                     text: "some context markdown"
                   }
                 ]
               }
             ]
           } = to_body([], context: "some context markdown")

    assert %{
             blocks: [
               %{type: "context"},
               _,
               _
             ]
           } = to_body([%{a: "b"}, %{a: "b", c: "d"}], context: "some context markdown")
  end
end
