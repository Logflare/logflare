defmodule Logflare.Backends.SlackAdaptorTest do
  @moduledoc false
  use Logflare.DataCase, async: false
  import Logflare.Backends.Adaptor.SlackAdaptor
  alias Logflare.Backends.Adaptor.SlackAdaptor
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

    # timestamp handling
    unix = DateTime.utc_now() |> DateTime.to_unix(:microsecond)

    assert [%{text: "timestamp:"}, %{text: " "}, %{text: ts}] =
             to_rich_text_preformatted(%{"timestamp" => unix})

    {:ok, microsecond} = DateTime.from_unix(unix, :microsecond)
    assert ts == DateTime.to_string(microsecond)
  end

  test "to_rich_text_preformatted/1 with numbers" do
    assert [%{text: "123:"}, %{text: " "}, %{text: "123"}] =
             to_rich_text_preformatted(%{"123" => 123})

    assert [%{text: "123:"}, %{text: " "}, %{text: "123.2"}] =
             to_rich_text_preformatted(%{"123" => 123.2})
  end

  test "to_rich_text_preformatted/1 with nil" do
    assert [] =
             to_rich_text_preformatted(%{"123" => nil})
  end

  test "to_rich_text_preformatted/1 with maps" do
    assert [%{text: "123:"}, %{text: " "}, %{text: "{\"test\":\"test\"}"}] =
             to_rich_text_preformatted(%{"123" => %{"test" => "test"}})

    assert [%{text: "123:"}, %{text: " "}, %{text: "{\"test\":\"test\"}"}] =
             to_rich_text_preformatted(%{"123" => %{test: "test"}})
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

    assert %{
             blocks: [
               %{
                 type: "section",
                 text: %{
                   type: "mrkdwn",
                   text: "some markdown text"
                 },
                 accessory: %{type: "button", text: %{text: "some text"}}
               },
               _
             ]
           } =
             to_body([%{}],
               button_link: %{
                 markdown_text: "some markdown text",
                 text: "some text",
                 url: "some url"
               }
             )
  end

  test "SlackHookServer compat" do
    source = insert(:source, user: build(:user))
    le = build(:log_event, source: source)

    assert %{
             blocks: [
               %{
                 type: "section",
                 text: %{text: "*5 new event(s)*" <> _},
                 accessory: %{type: "button", text: %{text: "View events"}}
               },
               %{
                 type: "rich_text",
                 elements: [
                   %{
                     elements: [%{text: text}, %{text: " "}, %{text: msg}]
                   }
                 ]
               }
             ]
           } = SlackAdaptor.build_message(source, [le], 5)

    refute text =~ Integer.to_string(le.body["timestamp"])
    assert msg == le.body["event_message"]
  end
end
