defmodule Logflare.Sources.SourceTest do
  use Logflare.DataCase

  alias Logflare.Google.BigQuery.GCPConfig
  alias Logflare.Sources
  alias Logflare.Sources.Source

  doctest Logflare.Sources.Source, import: true

  setup do
    insert(:plan, name: "Free")
    :ok
  end

  describe "default_search_lql" do
    test "allows clearing default search LQL" do
      source = %Source{name: "Test Source", default_search_lql: "s:m.level"}

      changeset = Source.update_by_user_changeset(source, %{"default_search_lql" => ""})

      assert changeset.valid?
      assert apply_changes(changeset).default_search_lql == nil
    end
  end

  describe "transform_drop_fields validation" do
    for {label, input} <- [
          {"id", "id"},
          {"event_message", "event_message"},
          {"timestamp", "timestamp"},
          {"reserved field mixed with allowed entries", "service\nid\nm.routing.region"}
        ] do
      test "rejects dropping #{label}" do
        source = %Source{name: "Test Source"}

        changeset =
          Source.update_by_user_changeset(source, %{"transform_drop_fields" => unquote(input)})

        assert {"cannot drop reserved fields: " <> _, _} =
                 changeset.errors[:transform_drop_fields]
      end
    end

    test "allows dropping nested paths that share a name with a reserved field" do
      source = %Source{name: "Test Source"}

      changeset =
        Source.update_by_user_changeset(source, %{
          "transform_drop_fields" => "metadata.id\nm.timestamp"
        })

      assert changeset.valid?
    end

    test "allows clearing the field" do
      source = %Source{name: "Test Source", transform_drop_fields: "service"}

      changeset =
        Source.update_by_user_changeset(source, %{"transform_drop_fields" => ""})

      assert changeset.valid?
    end
  end

  describe "Source" do
    test "generate_bq_table_id/1" do
      u = insert(:user)
      s = insert(:source, token: "44a6851a-9a6f-49ee-822f-12c6f17bedee", rules: [], user_id: u.id)

      s =
        Sources.get_by(id: s.id)
        |> Sources.preload_defaults()

      dataset_id_append = GCPConfig.dataset_id_append()

      assert Source.generate_bq_table_id(s) ==
               "`logflare-dev-238720`.`#{s.user_id}#{dataset_id_append}`.`44a6851a_9a6f_49ee_822f_12c6f17bedee`"
    end

    test "generate_bq_table_id/1 with custom bigquery_dataset_id" do
      u = insert(:user, bigquery_dataset_id: "test_custom_dataset_1")
      s = insert(:source, token: "44a6851a-9a6f-49ee-822f-12c6f17bedee", rules: [], user_id: u.id)

      s =
        Sources.get_by(id: s.id)
        |> Sources.preload_defaults()

      assert s.bq_table_id ==
               "`logflare-dev-238720`.`test_custom_dataset_1`.`44a6851a_9a6f_49ee_822f_12c6f17bedee`"
    end

    test "generate_bq_table_id/1 escapes backticks in legacy dataset_id values from the database" do
      u = insert(:user, bigquery_dataset_id: "evil`injection")
      s = insert(:source, token: "44a6851a-9a6f-49ee-822f-12c6f17bedee", rules: [], user_id: u.id)

      s =
        Sources.get_by(id: s.id)
        |> Sources.preload_defaults()

      result = Source.generate_bq_table_id(s)
      assert result =~ "\\`"
      refute result =~ "``"
    end
  end
end
