defmodule Logflare.LogEventTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent}

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [source: source, user: user]
  end

  @valid_params %{"event_message" => "something", "metadata" => %{"my" => "key"}}
  test "make/2 from valid params", %{source: source} do
    params = @valid_params

    assert %LogEvent{
             body: body,
             drop: false,
             id: id,
             ingested_at: _,
             is_from_stale_query: nil,
             params: ^params,
             source: %_{},
             sys_uint: _,
             valid: true,
             pipeline_error: nil,
             via_rule: nil
           } = LogEvent.make(@valid_params, %{source: source})

    assert id == body["id"]
    assert body["metadata"]["my"] == "key"
  end

  test "make/2 cast custom param values", %{source: source} do
    params =
      Map.merge(@valid_params, %{
        "valid" => false,
        "pipeline_error" => "some error"
      })

    assert %LogEvent{
             drop: false,
             # validity gets overwritten
             valid: true,
             pipeline_error: nil,
             source: %_{}
           } = LogEvent.make(params, %{source: source})
  end

  test "make_from_db/2", %{source: source} do
    params = %{"metadata" => []}
    assert %{body: body} = LogEvent.make_from_db(params, %{source: source})
    # metadata should be rejected
    assert body["metadata"] == nil

    params = %{"metadata" => [%{"some" => "value"}]}
    le = LogEvent.make_from_db(params, %{source: source})
    assert %{body: %{"metadata" => %{"some" => "value"}}, source: %_{}} = le
    assert le.body["event_message"] == nil
  end

  test "apply_custom_event_message/1 generates custom event message from source setting", %{
    source: source
  } do
    params = %{
      "event_message" => "some message",
      "metadata" => %{"a" => "value"}
    }

    le =
      LogEvent.make(params, %{
        source: %{source | custom_event_message_keys: "id, event_message, m.a"}
      })

    le = LogEvent.apply_custom_event_message(le)
    assert le.body["event_message"] =~ le.id
    assert le.body["event_message"] =~ "value"
    assert le.body["event_message"] =~ "some message"
    assert le.body["message"] == nil
  end
end
