defmodule Logflare.LogEventTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.{LogEvent}

  setup do
    user = insert(:user)
    source = insert(:source, user_id: user.id)
    [source: source, user: user]
  end

  @valid_params %{"message" => "something", "metadata"=> %{"my"=> "key"}}
  test "make/2 from valid params", %{source: source} do
    params = @valid_params

    assert %LogEvent{
             body: body,
             drop: false,
             ephemeral: nil,
             id: id,
             ingested_at: _,
             is_from_stale_query: nil,
             make_from: nil,
             params: ^params,
             source: %_{},
             sys_uint: _,
             valid: true,
             validation_error: "",
             via_rule: nil
           } = LogEvent.make(@valid_params, %{source: source})

    assert id == body.id
    assert body.metadata["my"] == "key"
  end

  test "make/2 cast custom param values", %{source: source} do
    params = Map.merge(@valid_params, %{ "make_from"=> "custom", "valid"=> false, "validation_error"=> "some error"})
    assert %LogEvent{
      drop: false,
      ephemeral: nil,
      # validity gets overwritten
      valid: true,
      validation_error: "",
    } = LogEvent.make(params, %{source: source})
  end

  test "make_from_db/2", %{source: source} do
    params = %{metadata: []}
    assert %{body: %{metadata: %{}}, make_from: "db"} = LogEvent.make_from_db(params, %{source: source})

    params = %{metadata: [%{"some"=> "value"}]}
    assert %{body: %{metadata: %{"some"=> "value"}}} = LogEvent.make_from_db(params, %{source: source})
  end

  test "apply_custom_event_message/1 generates custom event message from source setting", %{source: source} do
    params = %{
      "message"=> "some message",
      "metadata"=> %{"a"=> "value"}
    }
    le =  LogEvent.make(params, %{source: %{source | custom_event_message_keys: "id, message, m.a"}})
    le = LogEvent.apply_custom_event_message(le)
    assert le.body.message =~ le.id
    assert le.body.message =~ "value"
    assert le.body.message =~ "some message"
  end
end
