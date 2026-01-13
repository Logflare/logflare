defmodule Logflare.Backends.SlackAdaptorTest do
  use Logflare.DataCase, async: true

  alias Logflare.Backends.Adaptor.SlackAdaptor

  doctest Logflare.Backends.Adaptor.SlackAdaptor, import: true

  describe "send_message/1" do
    test "can work with alert query" do
      alert = insert(:alert)

      pid = self()

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> expect(:send, fn _url, body ->
        send(pid, {:blocks, body.blocks})
        {:ok, %Tesla.Env{}}
      end)

      assert {:ok, _} = SlackAdaptor.send_message(alert, [])

      TestUtils.retry_assert(fn ->
        assert_received {:blocks, blocks}

        assert blocks == [
                 %{
                   type: "section",
                   accessory: %{
                     type: "button",
                     text: %{type: "plain_text", text: "Manage"},
                     url: "http://localhost:4000/alerts/#{alert.id}",
                     style: "primary"
                   },
                   text: %{
                     type: "mrkdwn",
                     text: "🔊 *#{alert.name}* | http://localhost:4000/alerts/#{alert.id}"
                   }
                 }
               ]
      end)

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> expect(:send, fn _url, body ->
        send(pid, {:blocks, body.blocks})

        {:ok, %Tesla.Env{}}
      end)

      assert {:ok, _} =
               SlackAdaptor.send_message(alert, [
                 %{"test title" => "message body"},
                 %{"another title" => "another body"}
               ])

      TestUtils.retry_assert(fn ->
        assert_received {:blocks, blocks}

        assert blocks == [
                 %{
                   type: "section",
                   accessory: %{
                     type: "button",
                     text: %{type: "plain_text", text: "Manage"},
                     url: "http://localhost:4000/alerts/#{alert.id}",
                     style: "primary"
                   },
                   text: %{
                     type: "mrkdwn",
                     text: "🔊 *#{alert.name}*, 2 rows | http://localhost:4000/alerts/#{alert.id}"
                   }
                 },
                 %{
                   type: "rich_text",
                   elements: [
                     %{
                       type: "rich_text_preformatted",
                       elements: [
                         %{type: "text", text: "test title:"},
                         %{type: "text", text: " "},
                         %{type: "text", text: "message body"}
                       ]
                     }
                   ]
                 },
                 %{
                   type: "rich_text",
                   elements: [
                     %{
                       type: "rich_text_preformatted",
                       elements: [
                         %{type: "text", text: "another title:"},
                         %{type: "text", text: " "},
                         %{type: "text", text: "another body"}
                       ]
                     }
                   ]
                 }
               ]
      end)
    end

    test "can work with any payload" do
      test_url = "http://example.com/test"

      pid = self()

      Logflare.Backends.Adaptor.SlackAdaptor.Client
      |> expect(:send, fn url, body ->
        send(pid, {:url, url})
        send(pid, {:body, body})

        {:ok, %Tesla.Env{}}
      end)

      assert {:ok, _} =
               SlackAdaptor.send_message(test_url, [
                 %{"test title" => "message body"},
                 %{"another title" => "another body"}
               ])

      TestUtils.retry_assert(fn ->
        assert_received {:url, ^test_url}
      end)

      TestUtils.retry_assert(fn ->
        assert_received {:body, body}

        assert body == %{
                 blocks: [
                   %{
                     type: "rich_text",
                     elements: [
                       %{
                         type: "rich_text_preformatted",
                         elements: [
                           %{type: "text", text: "test title:"},
                           %{type: "text", text: " "},
                           %{type: "text", text: "message body"}
                         ]
                       }
                     ]
                   },
                   %{
                     type: "rich_text",
                     elements: [
                       %{
                         type: "rich_text_preformatted",
                         elements: [
                           %{type: "text", text: "another title:"},
                           %{type: "text", text: " "},
                           %{type: "text", text: "another body"}
                         ]
                       }
                     ]
                   }
                 ]
               }
      end)
    end
  end

  test "to_rich_text_preformatted/1" do
    assert [%{text: "test:"}, %{text: " "}, %{text: "test"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"test" => "test"})

    assert [%{text: "test:"}, %{text: "\n"}, %{text: "test\ntest"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"test" => "test\ntest"})

    # multi-key
    assert [
             %{text: "a:"},
             %{text: " "},
             %{text: "a"},
             %{text: "\n"},
             %{text: "b:"},
             %{text: " "},
             %{text: "b"}
           ] = SlackAdaptor.to_rich_text_preformatted(%{"a" => "a", "b" => "b"})

    # url handling
    assert [%{text: "test:"}, %{text: " "}, %{url: "http://" <> _}] =
             SlackAdaptor.to_rich_text_preformatted(%{"test" => "http://test.com"})

    assert [%{text: "test:"}, %{text: " "}, %{url: "https://" <> _}] =
             SlackAdaptor.to_rich_text_preformatted(%{"test" => "https://test.com"})

    # timestamp handling
    unix = DateTime.utc_now() |> DateTime.to_unix(:microsecond)

    assert [%{text: "timestamp:"}, %{text: " "}, %{text: ts}] =
             SlackAdaptor.to_rich_text_preformatted(%{"timestamp" => unix})

    {:ok, microsecond} = DateTime.from_unix(unix, :microsecond)
    assert ts == DateTime.to_string(microsecond)
  end

  test "to_rich_text_preformatted/1 with numbers" do
    assert [%{text: "123:"}, %{text: " "}, %{text: "123"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"123" => 123})

    assert [%{text: "123:"}, %{text: " "}, %{text: "123.2"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"123" => 123.2})
  end

  test "to_rich_text_preformatted/1 with nil" do
    assert [] = SlackAdaptor.to_rich_text_preformatted(%{"123" => nil})
  end

  test "to_rich_text_preformatted/1 skips nil event values" do
    assert [%{text: "message:"}, %{text: " "}, %{text: "ok"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"message" => "ok", "status" => nil})
  end

  test "to_rich_text_preformatted/1 with non-string values" do
    assert [%{text: "status:"}, %{text: " "}, %{text: ":ok"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"status" => :ok})
  end

  test "to_rich_text_preformatted/1 with maps" do
    assert [%{text: "123:"}, %{text: " "}, %{text: "{\"test\":\"test\"}"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"123" => %{"test" => "test"}})

    assert [%{text: "123:"}, %{text: " "}, %{text: "{\"test\":\"test\"}"}] =
             SlackAdaptor.to_rich_text_preformatted(%{"123" => %{test: "test"}})
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
           } = SlackAdaptor.to_body([], context: "some context markdown")

    assert %{
             blocks: [
               %{type: "context"},
               _,
               _
             ]
           } =
             SlackAdaptor.to_body([%{a: "b"}, %{a: "b", c: "d"}],
               context: "some context markdown"
             )

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
             SlackAdaptor.to_body([%{}],
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
